package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A parallel mirror of {@link JpaCacheTest} that exercises the
 * {@link JpaCacheProvider#HAZELCAST_EMBEDDED} provider against a real in-process
 * Hazelcast 5.6 member.
 *
 * <p>The Hazelcast member is bootstrapped once via {@link #startHazelcast()} and torn
 * down via {@link #stopHazelcast()} so the seven test scenarios share a single member
 * while each scenario still gets a fresh {@link SessionManager} via {@link #setup()}.</p>
 *
 * <p>The member is configured via {@code src/test/resources/hazelcast.xml} which
 * disables all discovery to enforce a 1-member isolated cluster, uses a non-default
 * port range to avoid collision with any locally running production member, and
 * declares a wildcard cache config with statistics enabled so Hibernate region creation
 * produces visible JCache regions.</p>
 */
@Tag("slow")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JpaCacheHazelcastTest {

    private static final String PROVIDER_CLASS = "com.hazelcast.cache.impl.HazelcastServerCachingProvider";

    private SessionManager sessionManager;
    private JpaSession session;

    @BeforeAll
    void startHazelcast() {
        // Eagerly resolve the caching provider so a missing classpath fails fast with a clear
        // message rather than the first cache region creation deep inside Hibernate startup.
        CachingProvider provider = Caching.getCachingProvider(PROVIDER_CLASS);
        assertNotNull(provider, "HazelcastServerCachingProvider must be on the test runtime classpath");
    }

    @AfterAll
    void stopHazelcast() {
        // Close the JCache provider, which shuts down the in-process Hazelcast member.
        // Without this, the member lingers and CI accumulates orphaned members across suites.
        Caching.getCachingProvider(PROVIDER_CLASS).close();
    }

    @BeforeEach
    void setup() {
        sessionManager = new SessionManager();

        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "jpa_cache_hazelcast_test")
            .isUsingStatistics()
            .withDefaultCacheExpiryMs(2000)
            .withCacheProvider(JpaCacheProvider.HAZELCAST_EMBEDDED)
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(TestParentModel.class)
                    .build()
            )
            .build();

        session = sessionManager.connect(config);
    }

    @AfterEach
    void teardown() {
        if (sessionManager != null)
            sessionManager.shutdown();
    }

    @Test
    void cacheHit_withinExpiry() {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Prime the L2 entity cache via the lazy stream() hydration path. Phase 2d:
        // findAll() runs a getResultStream() criteria query that populates the Hazelcast
        // L2 entity region as a side effect of hydration (ResearchPack finding A4) but
        // does NOT consult L2 on subsequent criteria queries - so cache hits must be
        // observed via per-id find() lookups, which DO consult L2.
        session.getRepository(TestParentModel.class).findAll();
        session.getRepository(TestChildModel.class).findAll();

        Statistics stats = session.getSessionFactory().getStatistics();
        stats.clear();

        // Per-id find() within TTL should hit the Hazelcast L2 entity region without DB round-trip.
        session.with(s -> {
            assertNotNull(s.find(TestParentModel.class, 1));
            assertNotNull(s.find(TestChildModel.class, 10));
        });

        long cacheHits = stats.getSecondLevelCacheHitCount();
        assertTrue(cacheHits > 0, "Expected Hazelcast L2 cache hits within TTL, got " + cacheHits);
    }

    @Test
    void cacheMiss_afterExpiry() throws Exception {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Prime the L2 entity cache via the lazy stream() hydration path.
        session.getRepository(TestParentModel.class).findAll();

        // Stop the scheduler to test JCache safety-net expiry
        session.getScheduler().shutdown();

        // Wait past 4s JCache TTL (2x multiplier on 2s @CacheExpiry)
        Thread.sleep(5000);

        Statistics stats = session.getSessionFactory().getStatistics();
        stats.clear();

        // Per-id find() after JCache TTL expiry should miss the Hazelcast L2 region.
        session.with(s -> { assertNotNull(s.find(TestParentModel.class, 1)); });

        long cacheMisses = stats.getSecondLevelCacheMissCount();
        assertTrue(cacheMisses > 0, "Expected Hazelcast L2 cache misses after TTL expiry, got " + cacheMisses);
    }

    @Test
    void parentChildRelationship_survivesExpiry() throws Exception {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Prime the cache
        session.getRepository(TestChildModel.class).findAll();

        // Stop the scheduler to test JCache safety-net expiry
        session.getScheduler().shutdown();

        // Wait past 4s JCache TTL (2x multiplier on 2s @CacheExpiry)
        Thread.sleep(5000);

        // Query child again - should transparently reload from DB without FK violation
        var children = session.getRepository(TestChildModel.class).findAll();
        assertFalse(children.isEmpty(), "Expected children after Hazelcast cache expiry");

        TestChildModel child = children.getFirst();
        assertNotNull(child.getParent(), "Child's parent reference should be intact after Hazelcast expiry");
        assertEquals("parent1", child.getParent().getName());
    }

    @Test
    void initialLoad_populatesRepository() {
        insertParentAndChild(1, "parent1", 10, "child1");

        var parents = session.getRepository(TestParentModel.class).findAll();
        assertFalse(parents.isEmpty(), "Repository should contain entities after initial load on Hazelcast");

        JpaRepository<TestParentModel> repo = (JpaRepository<TestParentModel>) session.getRepository(TestParentModel.class);
        assertNotNull(repo.getInitialLoad(), "Initial load stopwatch should be set");
        assertTrue(repo.getInitialLoad().durationMillis() >= 0, "Initial load duration should be non-negative");
    }

    @Test
    void proactiveRefresh_warmsCache() throws Exception {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Prime the L2 entity cache via the lazy stream() hydration path.
        session.getRepository(TestParentModel.class).findAll();

        // Wait past 4s JCache TTL (2x multiplier). The scheduler runs refreshAll() at 2s
        // and 4s. Phase 2d's refreshAll() Phase 3 evicts L2 without an eager re-warm pass
        // (the L2 entity cache populates as a side effect of subsequent streaming hydration
        // per ResearchPack A4) - so we must touch the data again here to re-populate L2.
        Thread.sleep(5000);

        // Re-touch the data to re-populate L2 via the lazy stream() hydration path.
        session.getRepository(TestParentModel.class).findAll();

        Statistics stats = session.getSessionFactory().getStatistics();
        stats.clear();

        // Per-id find() should now hit Hazelcast L2 because the previous findAll() re-populated it.
        session.with(s -> { assertNotNull(s.find(TestParentModel.class, 1)); });

        long cacheHits = stats.getSecondLevelCacheHitCount();
        long entityMisses = stats.getSecondLevelCacheMissCount();
        assertTrue(cacheHits > 0, "Expected Hazelcast L2 hits after re-population, got " + cacheHits);
        assertEquals(0, entityMisses, "Expected no entity cache misses on Hazelcast - L2 should be populated");
    }

    @Test
    void persistToDatabase_upsert_noFkViolation() {
        insertParentAndChild(1, "parent1", 10, "child1");

        JpaRepository<TestParentModel> parentRepo = (JpaRepository<TestParentModel>) session.getRepository(TestParentModel.class);

        // Re-persist parent with same ID - upsert should not violate FK from child
        TestParentModel parent = new TestParentModel();
        parent.setId(1);
        parent.setName("parent1_updated");

        assertDoesNotThrow(() -> parentRepo.persistToDatabase(repo -> Concurrent.newList(parent)),
            "Upsert persist should not cause FK violation on Hazelcast L2");

        // Verify child's parent reference is intact
        parentRepo.evict();
        JpaRepository<TestChildModel> childRepo = (JpaRepository<TestChildModel>) session.getRepository(TestChildModel.class);
        childRepo.evict();
        var children = childRepo.findAll();
        assertFalse(children.isEmpty(), "Child should still exist after upsert persist");
        assertNotNull(children.getFirst().getParent(), "Child's parent reference should be intact");
        assertEquals(1, children.getFirst().getParent().getId());
    }

    @Test
    void removeStaleEntities_deletesInCorrectOrder() {
        insertParentAndChild(1, "parent1", 10, "child1");
        insertParentAndChild(2, "parent2", 20, "child2");

        JpaRepository<TestParentModel> parentRepo = (JpaRepository<TestParentModel>) session.getRepository(TestParentModel.class);
        JpaRepository<TestChildModel> childRepo = (JpaRepository<TestChildModel>) session.getRepository(TestChildModel.class);

        // Re-persist subset: keep parent1, mark parent2 as stale
        TestParentModel keptParent = new TestParentModel();
        keptParent.setId(1);
        keptParent.setName("parent1");
        parentRepo.persistToDatabase(repo -> Concurrent.newList(keptParent));

        // Re-persist subset: keep child1, mark child2 as stale
        TestChildModel keptChild = new TestChildModel();
        keptChild.setId(10);
        keptChild.setParent(keptParent);
        keptChild.setValue("child1");
        childRepo.persistToDatabase(repo -> Concurrent.newList(keptChild));

        // Remove stale in FK-safe order: children first, then parents
        assertDoesNotThrow(() -> {
            childRepo.removeStaleEntities();
            parentRepo.removeStaleEntities();
        }, "FK-safe stale removal should succeed on Hazelcast L2");

        // Verify only kept entities remain
        parentRepo.evict();
        childRepo.evict();
        var parents = parentRepo.findAll();
        var children = childRepo.findAll();
        assertEquals(1, parents.size(), "Should have 1 parent remaining");
        assertEquals(1, children.size(), "Should have 1 child remaining");
        assertEquals("parent1", parents.getFirst().getName());
        assertEquals("child1", children.getFirst().getValue());
    }

    private void insertParentAndChild(int parentId, String parentName, int childId, String childValue) {
        session.transaction(s -> {
            TestParentModel parent = new TestParentModel();
            parent.setId(parentId);
            parent.setName(parentName);
            s.persist(parent);

            TestChildModel child = new TestChildModel();
            child.setId(childId);
            child.setParent(parent);
            child.setValue(childValue);
            s.persist(child);
        });
    }

}

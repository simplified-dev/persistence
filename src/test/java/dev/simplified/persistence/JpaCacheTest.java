package dev.simplified.persistence;

import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.collection.Concurrent;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("slow")
class JpaCacheTest {

    private SessionManager sessionManager;
    private JpaSession session;

    @BeforeEach
    void setup() {
        sessionManager = new SessionManager();

        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "jpa_cache_test")
            .isUsingStatistics()
            .withDefaultCacheExpiryMs(2000)
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

        // Prime the cache
        session.getRepository(TestParentModel.class).findAll();
        session.getRepository(TestChildModel.class).findAll();

        Statistics stats = session.getSessionFactory().getStatistics();
        stats.clear();

        // Query again - should hit L2/query cache
        session.getRepository(TestParentModel.class).findAll();
        session.getRepository(TestChildModel.class).findAll();

        long cacheHits = stats.getQueryCacheHitCount() + stats.getSecondLevelCacheHitCount();
        assertTrue(cacheHits > 0, "Expected cache hits within TTL, got " + cacheHits);
    }

    @Test
    void cacheMiss_afterExpiry() throws Exception {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Prime the cache
        session.getRepository(TestParentModel.class).findAll();

        // Stop the scheduler to test JCache safety-net expiry
        session.getScheduler().shutdown();

        // Wait past 4s JCache TTL (2x multiplier on 2s @CacheExpiry)
        Thread.sleep(5000);

        Statistics stats = session.getSessionFactory().getStatistics();
        stats.clear();

        // Query again - cache entries should have expired without scheduler to re-warm
        session.getRepository(TestParentModel.class).findAll();

        long cacheMisses = stats.getQueryCacheMissCount() + stats.getSecondLevelCacheMissCount();
        assertTrue(cacheMisses > 0, "Expected cache misses after JCache TTL expiry, got " + cacheMisses);
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
        assertFalse(children.isEmpty(), "Expected children after cache expiry");

        TestChildModel child = children.getFirst();
        assertNotNull(child.getParent(), "Child's parent reference should be intact after cache expiry");
        assertEquals("parent1", child.getParent().getName());
    }

    @Test
    void initialLoad_populatesRepository() {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Re-cache to trigger initial load on a fresh repository
        // (The setup already called cacheRepositories, but we inserted data after)
        // Verify the repository is functional
        var parents = session.getRepository(TestParentModel.class).findAll();
        assertFalse(parents.isEmpty(), "Repository should contain entities after initial load");

        JpaRepository<TestParentModel> repo = (JpaRepository<TestParentModel>) session.getRepository(TestParentModel.class);
        assertNotNull(repo.getInitialLoad(), "Initial load stopwatch should be set");
        assertTrue(repo.getInitialLoad().durationMillis() >= 0, "Initial load duration should be non-negative");
    }

    @Test
    void proactiveRefresh_warmsCache() throws Exception {
        insertParentAndChild(1, "parent1", 10, "child1");

        // Prime the cache
        session.getRepository(TestParentModel.class).findAll();

        // Wait past 4s JCache TTL (2x multiplier) but let the scheduler re-warm at 2s and 4s
        Thread.sleep(5000);

        Statistics stats = session.getSessionFactory().getStatistics();
        stats.clear();

        // Query again - scheduler should have kept the cache warm
        session.getRepository(TestParentModel.class).findAll();

        long cacheHits = stats.getQueryCacheHitCount() + stats.getSecondLevelCacheHitCount();
        long entityMisses = stats.getSecondLevelCacheMissCount();
        assertTrue(cacheHits > 0, "Expected cache hits from proactive refresh, got " + cacheHits);
        assertEquals(0, entityMisses, "Expected no entity cache misses - scheduler should have kept cache warm");
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
            "Upsert persist should not cause FK violation");

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
        }, "FK-safe stale removal should succeed when children are removed before parents");

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

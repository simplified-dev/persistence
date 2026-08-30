package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.store.WriteRequest;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A parallel mirror of {@link JpaCacheTest} that exercises the
 * {@link JpaCacheProvider#HAZELCAST_EMBEDDED} provider against a real in-process
 * Hazelcast 5.6 member.
 *
 * <p>The Hazelcast member is bootstrapped once via {@link #startHazelcast()} and torn
 * down via {@link #stopHazelcast()} so the test scenarios share a single member
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
                RepositoryFactory.of(TestParentModel.class)
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
    @DisplayName("a repository holds a generation once the session has connected")
    void connectPublishesAGeneration() {
        JpaRepository<TestParentModel> repository = (JpaRepository<TestParentModel>) this.session.getRepository(TestParentModel.class);

        assertEquals(HydrationState.CURRENT, repository.getState());
        assertNotNull(repository.getInitialLoad(), "the first hydration should be timed");
        assertTrue(repository.getInitialLoad().durationMillis() >= 0);
    }

    @Test
    @DisplayName("a write reaches the origin and the generation follows it")
    void writeRehydrates() {
        this.insertParentAndChild(1, "parent1", 10, "child1");

        ConcurrentList<TestParentModel> parents = this.session.getRepository(TestParentModel.class).findAll();
        assertFalse(parents.isEmpty(), "the written row should be held after the write rehydrates");
        assertEquals("parent1", parents.getFirst().getName());
    }

    @Test
    @DisplayName("a read answers from the held rows and issues no query")
    void readIssuesNoQuery() {
        this.insertParentAndChild(1, "parent1", 10, "child1");

        Statistics stats = this.session.getSessionFactory().getStatistics();
        stats.clear();

        // Every finder is written over the held generation, so none of them reaches a database.
        this.session.getRepository(TestParentModel.class).findAll();
        this.session.getRepository(TestParentModel.class).findFirst(TestParentModel::getName, "parent1");
        this.session.getRepository(TestChildModel.class).findAll();

        assertEquals(0, stats.getPrepareStatementCount(), "a read over held rows should prepare no statement");
    }

    @Test
    @DisplayName("a link resolved at hydration survives into the held rows")
    void linksAreResolvedBeforePublication() {
        this.insertParentAndChild(1, "parent1", 10, "child1");

        ConcurrentList<TestChildModel> children = this.session.getRepository(TestChildModel.class).findAll();
        assertFalse(children.isEmpty(), "expected the written child");

        TestChildModel child = children.getFirst();
        assertNotNull(child.getParent(), "the child's parent should be resolved in the held generation");
        assertEquals("parent1", child.getParent().getName());
    }

    @Test
    @DisplayName("direct session access still consults the second-level cache")
    void cacheHitWithinExpiry() {
        this.insertParentAndChild(1, "parent1", 10, "child1");

        Statistics stats = this.session.getSessionFactory().getStatistics();
        stats.clear();

        // The hydration the write triggered populated the entity region on its way past, so a per-id
        // find within the TTL answers from it. This is the escape hatch, not the repository path.
        this.session.with(hibernate -> {
            assertNotNull(hibernate.find(TestParentModel.class, 1));
            assertNotNull(hibernate.find(TestChildModel.class, 10));
        });

        long hits = stats.getSecondLevelCacheHitCount();
        assertTrue(hits > 0, "expected L2 entity cache hits within the TTL, got " + hits);
    }

    @Test
    @DisplayName("the second-level cache still expires on its own TTL")
    void cacheMissAfterExpiry() throws Exception {
        this.insertParentAndChild(1, "parent1", 10, "child1");

        // 4s JCache TTL, from the 2x multiplier on a 2s default expiry.
        Thread.sleep(5000);

        Statistics stats = this.session.getSessionFactory().getStatistics();
        stats.clear();

        this.session.with(hibernate -> { assertNotNull(hibernate.find(TestParentModel.class, 1)); });

        long misses = stats.getSecondLevelCacheMissCount();
        assertTrue(misses > 0, "expected L2 entity cache misses after the TTL, got " + misses);
    }

    private void insertParentAndChild(int parentId, String parentName, int childId, String childValue) {
        TestParentModel parent = new TestParentModel();
        parent.setId(parentId);
        parent.setName(parentName);
        this.session.write(WriteRequest.upsert(TestParentModel.class, List.of(parent)));

        TestChildModel child = new TestChildModel();
        child.setId(childId);
        child.setParent(parent);
        child.setValue(childValue);
        this.session.write(WriteRequest.upsert(TestChildModel.class, List.of(child)));
    }

}

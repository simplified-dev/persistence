package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.store.WriteRequest;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers what a relational type gets from holding a generation, and what the second-level cache is
 * still for once repository reads stop consulting it.
 */
@Tag("slow")
class JpaCacheTest {

    private SessionManager sessionManager;
    private JpaSession session;

    @BeforeEach
    void setup() {
        this.sessionManager = new SessionManager();

        JpaConfig config = JpaConfig.common(H2MemoryDriver.named("jpa_cache_test").isUsingStatistics().withDefaultCacheExpiryMs(2000).build())
            .withRepositoryFactory(RepositoryFactory.of(TestParentModel.class))
            .build();

        this.session = this.sessionManager.connect(config);
    }

    @AfterEach
    void teardown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
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

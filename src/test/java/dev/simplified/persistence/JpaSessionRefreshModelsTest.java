package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.source.Source;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies the Phase 5.5 {@link JpaSession#refreshModels(java.util.Collection)}
 * public API drives the 3-phase refresh (source reload, stale removal, L2 eviction)
 * for the specified model subset while skipping unregistered or unrelated models.
 *
 * <p>Each test wires a fresh session using two test models ({@link TestParentModel}
 * and {@link TestChildModel}) backed by counting {@link Source} lambdas so the test
 * can observe exactly how many times each model's source was reloaded. The counting
 * sources return a deterministic one-entity list on every call so the downstream
 * stale-removal and evict phases exercise non-trivial code paths.
 */
@Tag("slow")
class JpaSessionRefreshModelsTest {

    private SessionManager sessionManager;
    private JpaSession session;
    private AtomicInteger parentSourceCalls;
    private AtomicInteger childSourceCalls;

    @BeforeEach
    void setUp() {
        this.sessionManager = new SessionManager();
        this.parentSourceCalls = new AtomicInteger();
        this.childSourceCalls = new AtomicInteger();

        Source<TestParentModel> parentSource = repo -> {
            this.parentSourceCalls.incrementAndGet();
            TestParentModel parent = new TestParentModel();
            parent.setId(1);
            parent.setName("parent1");
            return Concurrent.newList(parent);
        };

        Source<TestChildModel> childSource = repo -> {
            this.childSourceCalls.incrementAndGet();
            TestChildModel child = new TestChildModel();
            child.setId(10);
            child.setValue("child1");
            // Parent is resolved via FK on the next query; leaving it null here matches the
            // JSON-seeded flow where the Source does not pre-wire ManyToOne references.
            return Concurrent.newList(child);
        };

        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "refresh_models_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(TestParentModel.class)
                    .with(TestParentModel.class, parentSource)
                    .with(TestChildModel.class, childSource)
                    .build()
            )
            .build();

        this.session = this.sessionManager.connect(config);

        // cacheRepositories() fires the initial load (one source call per model), so the
        // counters start at 1 each. Reset them here to keep the per-test assertions crisp.
        this.parentSourceCalls.set(0);
        this.childSourceCalls.set(0);
    }

    @AfterEach
    void tearDown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
    }

    @Test
    @DisplayName("empty target set is a no-op")
    void emptyTargetSetIsNoOp() {
        assertDoesNotThrow(() -> this.session.refreshModels(List.of()));
        assertThat(this.parentSourceCalls.get(), equalTo(0));
        assertThat(this.childSourceCalls.get(), equalTo(0));
    }

    @Test
    @DisplayName("single-model refresh invokes only the targeted source")
    void singleModelRefreshInvokesOnlyTargetedSource() {
        this.session.refreshModels(List.of(TestParentModel.class));

        assertThat(this.parentSourceCalls.get(), equalTo(1));
        assertThat(this.childSourceCalls.get(), equalTo(0));
    }

    @Test
    @DisplayName("multi-model refresh invokes every targeted source in topological order")
    void multiModelRefreshInvokesEveryTargetedSource() {
        this.session.refreshModels(List.of(TestParentModel.class, TestChildModel.class));

        assertThat(this.parentSourceCalls.get(), equalTo(1));
        assertThat(this.childSourceCalls.get(), equalTo(1));

        // Subsequent streaming hydration should see the freshly-loaded rows in both repositories.
        ConcurrentList<TestParentModel> parents = this.session.getRepository(TestParentModel.class).findAll();
        ConcurrentList<TestChildModel> children = this.session.getRepository(TestChildModel.class).findAll();

        assertThat(parents.size(), greaterThanOrEqualTo(1));
        assertThat(children.size(), greaterThanOrEqualTo(1));
    }

    @Test
    @DisplayName("unknown model class is silently skipped")
    void unknownModelClassIsSilentlySkipped() {
        // UnregisteredModel is not in the session's model list so refreshModels should
        // treat it as a no-op rather than throwing. This mirrors the Phase 5.5 AssetPoller
        // use case where a manifest may reference a class that has been renamed or removed
        // since the last schema revision.
        assertDoesNotThrow(() -> this.session.refreshModels(Set.of(UnregisteredModel.class)));
        assertThat(this.parentSourceCalls.get(), equalTo(0));
        assertThat(this.childSourceCalls.get(), equalTo(0));
    }

    @Test
    @DisplayName("mixed known and unknown targets refreshes only the registered subset")
    void mixedKnownAndUnknownTargetsRefreshesOnlyRegisteredSubset() {
        this.session.refreshModels(List.of(TestParentModel.class, UnregisteredModel.class));

        assertThat(this.parentSourceCalls.get(), equalTo(1));
        assertThat(this.childSourceCalls.get(), equalTo(0));
    }

    @Test
    @DisplayName("source failure on refresh is wrapped in JpaException")
    void sourceFailureIsWrappedInJpaException() {
        // Start from a clean session with a mutable source so the initial load succeeds,
        // then arm the failure for the subsequent refreshModels() call.
        this.sessionManager.shutdown();
        this.sessionManager = new SessionManager();

        AtomicInteger parentCalls = new AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean explode = new java.util.concurrent.atomic.AtomicBoolean(false);

        Source<TestParentModel> togglingSource = repo -> {
            parentCalls.incrementAndGet();

            if (explode.get())
                throw new IllegalStateException("boom");

            TestParentModel parent = new TestParentModel();
            parent.setId(1);
            parent.setName("parent1");
            return Concurrent.newList(parent);
        };

        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "refresh_models_failing_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(TestParentModel.class)
                    .with(TestParentModel.class, togglingSource)
                    .build()
            )
            .build();

        this.session = this.sessionManager.connect(config);

        // Initial load already ran one source call. Arm the next call to throw and verify
        // refreshModels wraps the failure in JpaException.
        explode.set(true);
        JpaException thrown = assertThrows(
            JpaException.class,
            () -> this.session.refreshModels(List.of(TestParentModel.class))
        );

        assertThat(thrown.getMessage(), equalTo("Failed to refresh data for 'TestParentModel'"));
    }

    /**
     * Placeholder class used only to assert that {@link JpaSession#refreshModels} silently
     * skips target entries whose class is not registered in the session. Deliberately NOT an
     * {@code @Entity} - the classpath scan must ignore it entirely.
     */
    private static final class UnregisteredModel implements JpaModel { }

}

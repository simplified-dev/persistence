package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.store.Source;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the hydration pass a session runs when it connects: every type reads once, links resolve
 * afterwards, and a failing origin is not quietly served as an empty one.
 */
@Tag("slow")
class JpaSessionHydrationTest {

    private SessionManager sessionManager;

    @AfterEach
    void tearDown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
    }

    /**
     * Connects a session over the two test models reading from the given source.
     */
    private @NotNull JpaSession connect(@NotNull String schema, @NotNull Source source) {
        this.sessionManager = new SessionManager();

        return this.sessionManager.connect(
            JpaConfig.common(new H2MemoryDriver(), schema)
                .withRepositoryFactory(RepositoryFactory.of(TestParentModel.class, source))
                .build()
        );
    }

    @Test
    @DisplayName("connecting reads every registered type exactly once")
    void connectReadsEveryTypeOnce() {
        AtomicInteger parentReads = new AtomicInteger();
        AtomicInteger childReads = new AtomicInteger();

        JpaSession session = this.connect("hydration_reads_once", new Source() {

            @Override
            @SuppressWarnings("unchecked")
            public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
                if (type == TestParentModel.class) {
                    parentReads.incrementAndGet();
                    TestParentModel parent = new TestParentModel();
                    parent.setId(1);
                    parent.setName("parent1");
                    return (ConcurrentList<T>) Concurrent.newList(parent);
                }

                if (type == TestChildModel.class) {
                    childReads.incrementAndGet();
                    TestChildModel child = new TestChildModel();
                    child.setId(10);
                    child.setValue("child1");
                    return (ConcurrentList<T>) Concurrent.newList(child);
                }

                return Concurrent.newUnmodifiableList();
            }

        });

        assertThat(parentReads.get(), equalTo(1));
        assertThat(childReads.get(), equalTo(1));

        // A read afterwards answers from the held generation, so the counters do not move.
        session.getRepository(TestParentModel.class).findAll();
        session.getRepository(TestChildModel.class).findAll();
        session.getRepository(TestParentModel.class).findFirst(TestParentModel::getName, "parent1");

        assertThat(parentReads.get(), equalTo(1));
        assertThat(childReads.get(), equalTo(1));
    }

    @Test
    @DisplayName("every type reports a published generation once connected")
    void everyTypeIsCurrentAfterConnect() {
        JpaSession session = this.connect("hydration_states", Source.none());

        assertThat(session.getRepository(TestParentModel.class).getState(), equalTo(HydrationState.CURRENT));
        assertThat(session.getRepository(TestChildModel.class).getState(), equalTo(HydrationState.CURRENT));
    }

    @Test
    @DisplayName("a failing origin fails the connect rather than serving an empty corpus")
    void failingOriginFailsTheConnect() {
        AtomicBoolean explode = new AtomicBoolean(true);

        JpaException thrown = assertThrows(
            JpaException.class,
            () -> this.connect("hydration_failure", new Source() {

                @Override
                public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
                    if (explode.get())
                        throw new IllegalStateException("boom");

                    return Concurrent.newUnmodifiableList();
                }

            })
        );

        // The failure has to name the type, because a pass over every model that reports only "boom"
        // says nothing about which origin is down. The pass aborts on the first type it reaches, and
        // the registration order is the discovery order rather than anything this test chooses.
        String firstRead = RepositoryFactory.resolveModels(TestParentModel.class).getFirst().getName();
        assertThat(thrown.getMessage().contains(firstRead), equalTo(true));
    }

}

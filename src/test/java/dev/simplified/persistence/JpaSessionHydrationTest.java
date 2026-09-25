package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.linked.LinkedParent;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.source.RelationalSource;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The hydration pass a session runs when it connects: every type reads once, links resolve
 * afterwards, the session is registered only once it has hydrated, a failing source is not quietly
 * served as an empty one, and a write that goes around the session leaves the rows it holds as they
 * were.
 */
@Tag("slow")
class JpaSessionHydrationTest {

    private SessionManager sessionManager;
    private RelationalSource database;

    @AfterEach
    void tearDown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();

        if (this.database != null)
            this.database.close();
    }

    /**
     * Connects a session over the test models reading from the given source.
     */
    private @NotNull JpaSession connect(@NotNull Source source) {
        this.sessionManager = new SessionManager();
        return this.sessionManager.connect(new JpaConfig(JpaModel.resolveModels(TestParentModel.class), source));
    }

    /**
     * Connects a session over the test models reading from a database this opens.
     */
    private @NotNull JpaSession connect(@NotNull String schema) {
        ConcurrentList<Class<JpaModel>> models = JpaModel.resolveModels(TestParentModel.class);
        this.database = H2MemoryDriver.named(schema)
            .withModels(models)
            .build();

        return this.connect(this.database);
    }

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    private static @NotNull TestParentModel parent(int id, @NotNull String name) {
        TestParentModel parent = new TestParentModel();
        parent.setId(id);
        parent.setName(name);
        return parent;
    }

    /**
     * Answers the name of the one parent row a session holds, failing when it holds any other count.
     *
     * @param session the session holding the parent type
     * @return the name of its one held parent row
     */
    private static @NotNull String heldName(@NotNull JpaSession session) {
        ConcurrentList<TestParentModel> held = session.getRepository(TestParentModel.class).orElseThrow().findAll();
        assertThat(held, hasSize(1));
        return held.getFirst().getName();
    }

    @Test
    @DisplayName("a lookup made while a session hydrates does not reach it")
    void registrationWaitsForHydration() {
        this.sessionManager = new SessionManager();
        AtomicReference<String> seen = new AtomicReference<>();

        this.sessionManager.connect(new JpaConfig(JpaModel.resolveModels(TestParentModel.class), new Source() {

            @Override
            public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
                try {
                    sessionManager.getRepository(TestParentModel.class);
                    seen.compareAndSet(null, "reached the session");
                } catch (JpaException exception) {
                    seen.compareAndSet(null, exception.getMessage());
                }

                return Concurrent.newUnmodifiableList();
            }

        }));

        assertThat(seen.get(), equalTo("There are no active sessions"));
        assertThat(this.sessionManager.isActive(), is(true));
    }

    @Test
    @DisplayName("a type the database maps but the session does not register is reached through the database")
    void aMappedTypeNeedNotBeRegistered() {
        this.database = H2MemoryDriver.named("hydration_mapped_only")
            .withModels(JpaModel.resolveModels(TestParentModel.class))
            .build();

        TestParentModel parent = new TestParentModel();
        parent.setId(1);
        parent.setName("parent1");
        TestChildModel child = new TestChildModel();
        child.setId(10);
        child.setParent(parent);
        child.setValue("child1");
        this.database.transaction(hibernate -> {
            hibernate.persist(parent);
            hibernate.persist(child);
        });

        this.sessionManager = new SessionManager();
        JpaSession session = this.sessionManager.connect(new JpaConfig(models(TestChildModel.class), this.database));

        assertThat(session.getRepository(TestChildModel.class).orElseThrow().getRows().getFirst().getParent().getName(), equalTo("parent1"));
        assertThat(session.getRepository(TestParentModel.class).isEmpty(), is(true));
        this.database.with(hibernate -> { assertNotNull(hibernate.find(TestParentModel.class, 1)); });
    }

    @Test
    @DisplayName("a write around the session leaves the held rows as they were, and the same write through it rebuilds them")
    void aWriteAroundTheSessionLeavesTheHeldRows() {
        JpaSession session = this.connect("hydration_bypassed");
        session.write(WriteRequest.upsert(TestParentModel.class, List.of(parent(1, "parent1"))));
        assertThat(heldName(session), equalTo("parent1"));

        // Renamed through the database's own Hibernate access, the row changes in the database and
        // nowhere else, so the session holding the type still serves the name it read.
        this.database.transaction(hibernate -> { hibernate.find(TestParentModel.class, 1).setName("renamed"); });
        String stored = this.database.with(hibernate -> {
            return hibernate.find(TestParentModel.class, 1).getName();
        });
        assertThat(stored, equalTo("renamed"));
        assertThat(heldName(session), equalTo("parent1"));

        session.write(WriteRequest.upsert(TestParentModel.class, List.of(parent(1, "renamed"))));
        assertThat(heldName(session), equalTo("renamed"));
    }

    @Test
    @DisplayName("a registered type the database does not map fails the connect, naming the type")
    void aRegisteredTypeMustBeMapped() {
        this.database = H2MemoryDriver.named("hydration_unmapped")
            .withModels(JpaModel.resolveModels(TestParentModel.class))
            .build();
        this.sessionManager = new SessionManager();

        JpaException thrown = assertThrows(
            JpaException.class,
            () -> this.sessionManager.connect(new JpaConfig(models(LinkedParent.class), this.database))
        );

        assertThat(thrown.getMessage(), containsString(LinkedParent.class.getName()));
        assertThat(this.sessionManager.isActive(), is(false));
    }

    @Test
    @DisplayName("connecting reads every registered type exactly once")
    void connectReadsEveryTypeOnce() {
        AtomicInteger parentReads = new AtomicInteger();
        AtomicInteger childReads = new AtomicInteger();

        JpaSession session = this.connect(new Source() {

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
        session.getRepository(TestParentModel.class).orElseThrow().findAll();
        session.getRepository(TestChildModel.class).orElseThrow().findAll();
        session.getRepository(TestParentModel.class).orElseThrow().findFirst(TestParentModel::getName, "parent1");

        assertThat(parentReads.get(), equalTo(1));
        assertThat(childReads.get(), equalTo(1));
    }

    @Test
    @DisplayName("every type reports a published generation once connected")
    void everyTypeIsCurrentAfterConnect() {
        // The session reads from the database this opened. An empty table still publishes a
        // generation.
        JpaSession session = this.connect("hydration_states");

        assertThat(session.getRepository(TestParentModel.class).orElseThrow().getState(), equalTo(HydrationState.CURRENT));
        assertThat(session.getRepository(TestChildModel.class).orElseThrow().getState(), equalTo(HydrationState.CURRENT));
    }

    @Test
    @DisplayName("a failing origin fails the connect rather than serving an empty corpus")
    void failingOriginFailsTheConnect() {
        AtomicBoolean explode = new AtomicBoolean(true);

        JpaException thrown = assertThrows(
            JpaException.class,
            () -> this.connect(new Source() {

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
        String firstRead = JpaModel.resolveModels(TestParentModel.class).getFirst().getName();
        assertThat(thrown.getMessage().contains(firstRead), equalTo(true));

        // A session whose first hydration failed is never registered, so no lookup reaches its
        // half-built repositories and the same configuration can simply connect again.
        assertThat(this.sessionManager.isActive(), equalTo(false));
        assertThrows(JpaException.class, () -> this.sessionManager.getRepository(TestParentModel.class));
    }

}

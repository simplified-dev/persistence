package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.linked.LinkedChild;
import dev.simplified.persistence.linked.LinkedParent;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers how a session rebuilds a generation: nothing is published before its links resolve, a
 * write rebuilds every type linking into the written one, a failed rebuild publishes nothing and says
 * so, and a write that names no rows rebuilds nothing.
 */
class JpaSessionRebuildTest {

    private Corpus corpus;
    private SessionManager sessionManager;
    private JpaSession session;

    @BeforeEach
    void connect() {
        this.corpus = new Corpus();
        this.corpus.parents.put("p1", "one");
        this.corpus.children.put("c1", "p1");

        this.sessionManager = new SessionManager();
        this.session = this.sessionManager.connect(new JpaConfig(JpaModel.resolveModels(LinkedParent.class), this.corpus));
    }

    @AfterEach
    void shutdown() {
        this.sessionManager.shutdown();
    }

    @Test
    @DisplayName("a generation is visible only once it is held, and its links resolve before that")
    void nothingIsPublishedBeforeItIsHeld() {
        JpaRepository<LinkedChild> repository = new JpaRepository<>(LinkedChild.class);
        ConcurrentList<LinkedChild> read = repository.hydrate(this.corpus);

        assertThat(read, hasSize(1));
        assertThat(repository.getRows(), empty());
        assertThat(repository.getState(), equalTo(HydrationState.HYDRATING));

        LinkedParent parent = this.session.getRepository(LinkedParent.class).getRows().getFirst();
        repository.link(read, target -> JpaModel.keyed(LinkedParent.class, List.of(parent)));

        assertThat(read.getFirst().getParent(), sameInstance(parent));
        assertThat(repository.getRows(), empty());

        repository.hold(read);

        assertThat(repository.getRows().getFirst().getParent(), sameInstance(parent));
        assertThat(repository.getState(), equalTo(HydrationState.CURRENT));
    }

    @Test
    @DisplayName("a connected child already points at the parent the session holds")
    void connectLinksAgainstTheHeldParent() {
        LinkedParent held = this.session.getRepository(LinkedParent.class).getRows().getFirst();
        LinkedChild child = this.session.getRepository(LinkedChild.class).getRows().getFirst();

        assertThat(child.getParent(), sameInstance(held));
    }

    @Test
    @DisplayName("a rebuild that fails publishes nothing and reports the failure")
    void aFailedRebuildKeepsThePreviousGeneration() {
        Repository<LinkedParent> parents = this.session.getRepository(LinkedParent.class);
        LinkedParent before = parents.getRows().getFirst();
        this.corpus.failing = LinkedParent.class;

        assertThrows(JpaException.class, () -> this.session.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno")))));

        assertThat(parents.getState(), equalTo(HydrationState.DEGRADED));
        assertThat(parents.getRows().getFirst(), sameInstance(before));
        assertThat(parents.getRows().getFirst().getName(), equalTo("one"));
    }

    @Test
    @DisplayName("a write to a linked type rebuilds every type that links to it")
    void aWriteRelinksItsDependents() {
        int childReads = this.corpus.childReads.get();

        this.session.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno"))));

        LinkedParent held = this.session.getRepository(LinkedParent.class).getRows().getFirst();
        LinkedChild child = this.session.getRepository(LinkedChild.class).getRows().getFirst();

        assertThat(held.getName(), equalTo("uno"));
        assertThat(child.getParent(), sameInstance(held));
        assertThat(this.corpus.childReads.get(), equalTo(childReads + 1));
    }

    @Test
    @DisplayName("a write to a type nothing links to rebuilds that type alone")
    void aWriteToALeafRebuildsOnlyItself() {
        int parentReads = this.corpus.parentReads.get();
        LinkedChild child = new LinkedChild();
        child.setId("c2");
        child.setParentId("p1");

        this.session.write(WriteRequest.upsert(LinkedChild.class, List.of(child)));

        assertThat(this.corpus.parentReads.get(), equalTo(parentReads));
        assertThat(this.session.getRepository(LinkedChild.class).getRows(), hasSize(2));
    }

    @Test
    @DisplayName("a dependent that fails to rebuild keeps the written type's previous generation too")
    void aFailingDependentPublishesNothing() {
        Repository<LinkedParent> parents = this.session.getRepository(LinkedParent.class);
        Repository<LinkedChild> children = this.session.getRepository(LinkedChild.class);
        LinkedParent before = parents.getRows().getFirst();
        this.corpus.failing = LinkedChild.class;

        assertThrows(JpaException.class, () -> this.session.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno")))));

        assertThat(parents.getState(), equalTo(HydrationState.DEGRADED));
        assertThat(children.getState(), equalTo(HydrationState.DEGRADED));
        assertThat(parents.getRows().getFirst(), sameInstance(before));
        assertThat(children.getRows().getFirst().getParent(), sameInstance(before));
    }

    @Test
    @DisplayName("a write naming no rows writes and rebuilds nothing")
    void anEmptyWriteRebuildsNothing() {
        int reads = this.corpus.parentReads.get();

        this.session.write(WriteRequest.upsert(LinkedParent.class, List.of()));

        assertThat(this.corpus.parentReads.get(), equalTo(reads));
    }

    private static @NotNull LinkedParent parent(@NotNull String id, @NotNull String name) {
        LinkedParent parent = new LinkedParent();
        parent.setId(id);
        parent.setName(name);
        return parent;
    }

    /**
     * An origin held in memory, answering fresh instances on every read the way a parsed document
     * does.
     */
    private static final class Corpus implements Source.Writable {

        private final @NotNull ConcurrentMap<String, String> parents = Concurrent.newLinkedMap();
        private final @NotNull ConcurrentMap<String, String> children = Concurrent.newLinkedMap();
        private final @NotNull AtomicInteger parentReads = new AtomicInteger();
        private final @NotNull AtomicInteger childReads = new AtomicInteger();
        private volatile Class<?> failing;

        @Override
        @SuppressWarnings("unchecked")
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            if (type == this.failing)
                throw new IllegalStateException("origin down for " + type.getSimpleName());

            if (type == LinkedParent.class) {
                this.parentReads.incrementAndGet();
                ConcurrentList<LinkedParent> rows = Concurrent.newList();
                this.parents.forEach((id, name) -> rows.add(parent(id, name)));
                return (ConcurrentList<T>) rows;
            }

            if (type == LinkedChild.class) {
                this.childReads.incrementAndGet();
                ConcurrentList<LinkedChild> rows = Concurrent.newList();

                this.children.forEach((id, parentId) -> {
                    LinkedChild child = new LinkedChild();
                    child.setId(id);
                    child.setParentId(parentId);
                    rows.add(child);
                });

                return (ConcurrentList<T>) rows;
            }

            return Concurrent.newUnmodifiableList();
        }

        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) {
            request.rows().forEach(row -> {
                if (row instanceof LinkedParent parent)
                    this.parents.put(parent.getId(), parent.getName());
                else if (row instanceof LinkedChild child)
                    this.children.put(child.getId(), child.getParentId());
            });
        }

    }

}

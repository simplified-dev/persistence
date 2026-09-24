package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.linked.LinkedCorpus;
import dev.simplified.persistence.linked.LinkedParent;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.persistence.unmapped.ContractRow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;

import static dev.simplified.persistence.linked.LinkedCorpus.parent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The registry's routing of lookups and writes across the sessions it holds - here a read-only
 * session registered first and a writable one after it, the layout a consumer with one corpus and one
 * database of its own ends up with - and its reuse once it has been shut down to empty.
 */
class SessionManagerTest {

    private LinkedCorpus corpus;
    private SessionManager sessionManager;
    private JpaSession readOnly;
    private JpaSession writable;

    @BeforeEach
    void connect() {
        this.corpus = new LinkedCorpus();
        this.corpus.parents.put("p1", "one");

        this.sessionManager = new SessionManager();
        this.readOnly = this.sessionManager.connect(new JpaConfig(JpaModel.resolveModels(ContractRow.class), new Source() {

            @Override
            public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
                return Concurrent.newUnmodifiableList();
            }

        }));
        this.writable = this.sessionManager.connect(new JpaConfig(JpaModel.resolveModels(LinkedParent.class), this.corpus));
    }

    @AfterEach
    void shutdown() {
        this.sessionManager.shutdown();
    }

    @Test
    @DisplayName("a write goes to the session registering its type, past one that does not")
    void aWriteReachesTheSessionHoldingItsType() {
        int reads = this.corpus.readsOf(LinkedParent.class);

        this.sessionManager.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno"))));

        assertThat(this.corpus.parents.get("p1"), equalTo("uno"));
        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(reads + 1));
        assertThat(this.sessionManager.getRepository(LinkedParent.class).getRows().getFirst().getName(), equalTo("uno"));
    }

    @Test
    @DisplayName("a session over a read-only source refuses a write to its type")
    void aReadOnlySessionRefusesAWrite() {
        ContractRow row = new ContractRow();
        row.setId(1);

        JpaException thrown = assertThrows(JpaException.class, () -> this.sessionManager.write(WriteRequest.upsert(ContractRow.class, List.of(row))));

        assertThat(thrown.getMessage(), containsString("holds no write instruction"));
    }

    @Test
    @DisplayName("a type no session registers is written nowhere")
    void anUnregisteredTypeIsWrittenNowhere() {
        JpaException thrown = assertThrows(JpaException.class, () -> this.sessionManager.write(WriteRequest.upsert(TestParentModel.class, List.of(new TestParentModel()))));

        assertThat(thrown.getMessage(), containsString("No session holds"));
    }

    @Test
    @DisplayName("a session answers empty for a type it does not register, and the registry looks past it")
    void lookupsAnswerEmptyAndFallThrough() {
        assertThat(this.writable.getRepository(ContractRow.class).isEmpty(), is(true));
        assertThat(this.readOnly.getRepository(LinkedParent.class).isEmpty(), is(true));
        assertThat(this.sessionManager.getRepository(ContractRow.class), sameInstance(this.readOnly.getRepository(ContractRow.class).orElseThrow()));
        assertThat(this.sessionManager.getRepository(LinkedParent.class), sameInstance(this.writable.getRepository(LinkedParent.class).orElseThrow()));

        JpaException thrown = assertThrows(JpaException.class, () -> this.sessionManager.getRepository(TestParentModel.class));
        assertThat(thrown.getMessage(), containsString("Repository cannot be retrieved"));
    }

    @Test
    @DisplayName("a session that has been shut down answers nothing and takes no write")
    void aShutDownSessionIsGone() {
        this.sessionManager.shutdown(this.writable);

        assertThat(this.writable.getRepository(LinkedParent.class).isEmpty(), is(true));
        assertThrows(JpaException.class, () -> this.sessionManager.getRepository(LinkedParent.class));
        assertThrows(JpaException.class, () -> this.sessionManager.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno")))));
        assertThrows(JpaException.class, () -> this.writable.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno")))));
    }

    @Test
    @DisplayName("a manager shut down to empty connects again and serves the new session")
    void aManagerShutDownConnectsAgain() {
        this.sessionManager.shutdown();
        assertThat(this.sessionManager.isActive(), is(false));

        JpaSession again = this.sessionManager.connect(new JpaConfig(JpaModel.resolveModels(LinkedParent.class), this.corpus));

        assertThat(this.sessionManager.isActive(), is(true));
        assertThat(this.sessionManager.getRepository(LinkedParent.class), sameInstance(again.getRepository(LinkedParent.class).orElseThrow()));
        assertThat(this.sessionManager.getRepository(LinkedParent.class).getRows().getFirst().getName(), equalTo("one"));
    }

    @Test
    @DisplayName("a manager shut down to empty is no longer reachable through its shutdown hook")
    void aManagerShutDownToEmptyIsCollected() throws InterruptedException {
        ReferenceQueue<SessionManager> queue = new ReferenceQueue<>();
        WeakReference<SessionManager> reference = shutDownDetached(queue);
        Reference<? extends SessionManager> collected = null;

        // At most 50 collections, each followed by a 100ms wait for the reference to enqueue
        for (int attempt = 0; attempt < 50 && collected == null; attempt++) {
            System.gc();
            collected = queue.remove(100);
        }

        assertSame(reference, collected, "A manager shut down to empty is still reachable");
    }

    /**
     * Connects two sessions on a new manager and shuts it down to empty - one session through
     * {@link SessionManager#shutdown(JpaSession)}, the other through {@link SessionManager#shutdown()} -
     * handing back only a weak reference so no strong one outlives this frame.
     *
     * @param queue the queue the reference enqueues on once the manager is collected
     * @return a weak reference to the emptied manager
     */
    private static @NotNull WeakReference<SessionManager> shutDownDetached(@NotNull ReferenceQueue<SessionManager> queue) {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.parents.put("p1", "one");

        SessionManager manager = new SessionManager();
        JpaSession first = manager.connect(new JpaConfig(JpaModel.resolveModels(LinkedParent.class), corpus));
        manager.connect(new JpaConfig(JpaModel.resolveModels(LinkedParent.class), corpus));

        manager.shutdown(first);
        assertThat(manager.isActive(), is(true));
        manager.shutdown();
        assertThat(manager.isActive(), is(false));

        return new WeakReference<>(manager, queue);
    }

}

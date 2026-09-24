package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.linked.LinkedCorpus;
import dev.simplified.persistence.linked.LinkedParent;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.persistence.unfollowable.CollectionOwner;
import dev.simplified.persistence.unfollowable.ElementOwner;
import dev.simplified.persistence.unfollowable.LazyChild;
import dev.simplified.persistence.unfollowable.LazyOneToOne;
import dev.simplified.persistence.unfollowable.ManyToManyOwner;
import dev.simplified.persistence.unfollowable.WildcardLinked;
import dev.simplified.persistence.unmapped.ContractRow;
import dev.simplified.reflection.Reflection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static dev.simplified.persistence.linked.LinkedCorpus.parent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The registry's routing of lookups and writes across the sessions it holds - here a read-only
 * session registered first and a writable one after it, the layout a consumer with one corpus and one
 * database of its own ends up with - its reuse once it has been shut down to empty, the shutdown hook
 * that holds it while it holds a session, and the types it refuses to connect before anything is read.
 */
class SessionManagerTest {

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

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

        assertSame(reference, collected(queue, 50), "A manager shut down to empty is still reachable");
    }

    @Test
    @DisplayName("a manager holding a session stays reachable through its shutdown hook")
    void aManagerHoldingASessionIsHeldByItsHook() throws InterruptedException {
        ReferenceQueue<SessionManager> queue = new ReferenceQueue<>();
        WeakReference<SessionManager> reference = connectedDetached(queue);

        try {
            // Nothing but the hook holds the manager, so a collection that reaches it means the JVM
            // would not shut its session down at exit.
            assertThat("A manager holding a session was collected", collected(queue, 10), nullValue());
            assertThat(reference.get(), notNullValue());
        } finally {
            SessionManager manager = reference.get();

            if (manager != null)
                manager.shutdown();
        }
    }

    @Test
    @DisplayName("a registered type with a collection, element-collection or lazy association is refused at connect, naming the field, before anything is read")
    void anUnfollowableAssociationIsRefused() {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.parents.put("p1", "one");
        SessionManager manager = new SessionManager();
        Map<Class<?>, String> refusals = Map.of(
            CollectionOwner.class, "Field 'parents' of '%s' declares @OneToMany, which a held generation cannot follow",
            ManyToManyOwner.class, "Field 'parents' of '%s' declares @ManyToMany, which a held generation cannot follow",
            ElementOwner.class, "Field 'tags' of '%s' declares @ElementCollection, which a held generation cannot follow",
            LazyChild.class, "Field 'parent' of '%s' declares a lazy @ManyToOne, which a held generation cannot follow",
            LazyOneToOne.class, "Field 'parent' of '%s' declares a lazy @OneToOne, which a held generation cannot follow"
        );

        try {
            refusals.forEach((owner, message) -> {
                JpaException thrown = assertThrows(
                    JpaException.class,
                    () -> manager.connect(new JpaConfig(models(LinkedParent.class, owner), corpus)),
                    owner.getSimpleName()
                );

                assertThat(thrown.getMessage(), equalTo(String.format(message, owner.getName())));
            });

            assertThat(corpus.readsOf(LinkedParent.class), equalTo(0));
            assertThat(manager.isActive(), is(false));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("a wildcard @Linked list is refused at connect as a JpaException, not a ClassCastException")
    void aWildcardLinkIsRefused() {
        LinkedCorpus corpus = new LinkedCorpus();
        SessionManager manager = new SessionManager();

        try {
            JpaException thrown = assertThrows(
                JpaException.class,
                () -> manager.connect(new JpaConfig(models(LinkedParent.class, WildcardLinked.class), corpus))
            );

            assertThat(thrown.getMessage(), equalTo("Field 'parents' of '" + WildcardLinked.class.getName() + "' names no model it can resolve to"));
            assertThat(corpus.readsOf(LinkedParent.class), equalTo(0));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("a link or association resolves a plain model, an Optional of one or a list of one, and refuses every other shape as a JpaException")
    void aTargetResolvesOnlyAModel() {
        Reflection<Shapes> shapes = new Reflection<>(Shapes.class);

        for (String resolved : List.of("plain", "optional", "list", "javaList"))
            assertSame(LinkedParent.class, JpaRepository.targetOf(shapes.getField(resolved)), resolved);

        for (String refused : List.of("raw", "wildcard", "set", "arrayList", "map", "array", "notModel")) {
            JpaException thrown = assertThrows(JpaException.class, () -> JpaRepository.targetOf(shapes.getField(refused)), refused);
            assertThat(thrown.getMessage(), equalTo("Field '" + refused + "' of '" + Shapes.class.getName() + "' names no model it can resolve to"));
        }
    }

    /**
     * One field per shape a link or association might declare.
     */
    @SuppressWarnings({ "unused", "rawtypes" })
    private static final class Shapes {

        private LinkedParent plain;
        private Optional<LinkedParent> optional;
        private ConcurrentList<LinkedParent> list;
        private List<LinkedParent> javaList;
        private ConcurrentList raw;
        private Optional<? extends LinkedParent> wildcard;
        private Set<LinkedParent> set;
        private ArrayList<LinkedParent> arrayList;
        private Map<String, LinkedParent> map;
        private LinkedParent[] array;
        private String notModel;

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

    /**
     * Connects a session on a new manager and leaves it connected, handing back only a weak
     * reference so no strong one outlives this frame.
     *
     * @param queue the queue the reference enqueues on once the manager is collected
     * @return a weak reference to the manager holding the session
     */
    private static @NotNull WeakReference<SessionManager> connectedDetached(@NotNull ReferenceQueue<SessionManager> queue) {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.parents.put("p1", "one");

        SessionManager manager = new SessionManager();
        manager.connect(new JpaConfig(JpaModel.resolveModels(LinkedParent.class), corpus));
        assertThat(manager.isActive(), is(true));

        return new WeakReference<>(manager, queue);
    }

    /**
     * Runs collections until a reference enqueues, each followed by a 100ms wait for it.
     *
     * @param queue the queue a collected reference enqueues on
     * @param rounds the most collections to run
     * @return the reference that enqueued, or {@code null} when none did
     * @throws InterruptedException if a wait is interrupted
     */
    private static @Nullable Reference<? extends SessionManager> collected(
        @NotNull ReferenceQueue<SessionManager> queue,
        int rounds
    ) throws InterruptedException {
        Reference<? extends SessionManager> collected = null;

        for (int attempt = 0; attempt < rounds && collected == null; attempt++) {
            System.gc();
            collected = queue.remove(100);
        }

        return collected;
    }

}

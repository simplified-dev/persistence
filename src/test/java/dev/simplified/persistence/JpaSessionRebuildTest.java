package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.cycle.CycleA;
import dev.simplified.persistence.cycle.CycleB;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.linked.LinkedChild;
import dev.simplified.persistence.linked.LinkedCorpus;
import dev.simplified.persistence.linked.LinkedGrandchild;
import dev.simplified.persistence.linked.LinkedParent;
import dev.simplified.persistence.optional.LinkedStray;
import dev.simplified.persistence.sibling.LinkedSibling;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.persistence.subtype.SubtypeLinker;
import dev.simplified.persistence.subtype.SubtypeRow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static dev.simplified.persistence.linked.LinkedCorpus.child;
import static dev.simplified.persistence.linked.LinkedCorpus.grandchild;
import static dev.simplified.persistence.linked.LinkedCorpus.parent;
import static dev.simplified.persistence.linked.LinkedCorpus.sibling;
import static dev.simplified.persistence.linked.LinkedCorpus.stray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a session rebuilds a generation: nothing is published before its links resolve, a write
 * rebuilds every type linking into the written one, through a link declaring a supertype of it as
 * well, a landed write whose rebuild fails returns having
 * published nothing and says so on every covered type, a write the origin refuses throws and rebuilds
 * nothing, rebuilds run one at a time, and a write that names no rows rebuilds nothing.
 *
 * <p>It also pins what an id naming no row does: a plain link fails the connect or the rebuild and
 * refuses an upsert before it is written - so two new rows of different types naming each other
 * cannot be written at all - while an {@link Optional} link holds empty.
 *
 * <p>The session-level cases run twice, with the models registered parent first and child first, so
 * none of them rests on the order discovery happens to answer.
 */
class JpaSessionRebuildTest {

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    @Nested
    @DisplayName("registered parent first")
    class ParentFirst extends Rebuilds {

        ParentFirst() {
            super(models(LinkedParent.class, LinkedChild.class, LinkedGrandchild.class));
        }

    }

    @Nested
    @DisplayName("registered child first")
    class ChildFirst extends Rebuilds {

        ChildFirst() {
            super(models(LinkedGrandchild.class, LinkedChild.class, LinkedParent.class));
        }

    }

    /**
     * The session-level cases, over one registration order.
     */
    abstract class Rebuilds {

        private final @NotNull ConcurrentList<Class<JpaModel>> order;
        private LinkedCorpus corpus;
        private SessionManager sessionManager;
        private JpaSession session;

        Rebuilds(@NotNull ConcurrentList<Class<JpaModel>> order) {
            this.order = order;
        }

        @BeforeEach
        void connect() {
            this.corpus = new LinkedCorpus();
            this.corpus.parents.put("p1", "one");
            this.corpus.children.put("c1", "p1");
            this.corpus.grandchildren.put("g1", "c1");

            this.sessionManager = new SessionManager();
            this.session = this.sessionManager.connect(new JpaConfig(this.order, this.corpus));
        }

        @AfterEach
        void shutdown() {
            this.sessionManager.shutdown();
        }

        private <T extends JpaModel> @NotNull Repository<T> repository(@NotNull Class<T> type) {
            return this.session.getRepository(type).orElseThrow();
        }

        @Test
        @DisplayName("a connected row points at the instance its target's repository holds")
        void connectLinksAgainstTheHeldRows() {
            LinkedParent parent = this.repository(LinkedParent.class).getRows().getFirst();
            LinkedChild child = this.repository(LinkedChild.class).getRows().getFirst();
            LinkedGrandchild grandchild = this.repository(LinkedGrandchild.class).getRows().getFirst();

            assertThat(child.getParent(), sameInstance(parent));
            assertThat(grandchild.getChild(), sameInstance(child));
        }

        @Test
        @DisplayName("a write rebuilds every type linking into the written one, transitively")
        void aWriteRelinksItsDependents() {
            int childReads = this.corpus.readsOf(LinkedChild.class);
            int grandchildReads = this.corpus.readsOf(LinkedGrandchild.class);

            this.session.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno"))));

            LinkedParent parent = this.repository(LinkedParent.class).getRows().getFirst();
            LinkedChild child = this.repository(LinkedChild.class).getRows().getFirst();
            LinkedGrandchild grandchild = this.repository(LinkedGrandchild.class).getRows().getFirst();

            assertThat(parent.getName(), equalTo("uno"));
            assertThat(child.getParent(), sameInstance(parent));
            assertThat(grandchild.getChild(), sameInstance(child));
            assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(childReads + 1));
            assertThat(this.corpus.readsOf(LinkedGrandchild.class), equalTo(grandchildReads + 1));
        }

        @Test
        @DisplayName("a write to a type nothing links to rebuilds that type alone, against the held rows")
        void aWriteToALeafRebuildsOnlyItself() {
            LinkedChild held = this.repository(LinkedChild.class).getRows().getFirst();
            int parentReads = this.corpus.readsOf(LinkedParent.class);
            int childReads = this.corpus.readsOf(LinkedChild.class);

            this.session.write(WriteRequest.upsert(LinkedGrandchild.class, List.of(grandchild("g2", "c1"))));

            ConcurrentList<LinkedGrandchild> grandchildren = this.repository(LinkedGrandchild.class).getRows();
            assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(parentReads));
            assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(childReads));
            assertThat(grandchildren, hasSize(2));
            grandchildren.forEach(grandchild -> assertThat(grandchild.getChild(), sameInstance(held)));
        }

        @Test
        @DisplayName("a landed write whose rebuild fails to read returns, publishes nothing and reports the failure on every covered type")
        void aFailedReadKeepsEveryPreviousGeneration() {
            this.assertAFailedWriteKeepsEverything(() -> this.corpus.failing = LinkedParent.class);
        }

        @Test
        @DisplayName("a landed write whose rebuild fails to link returns, publishes nothing and reports the failure on every covered type")
        void aFailedLinkKeepsEveryPreviousGeneration() {
            this.assertAFailedWriteKeepsEverything(() -> this.corpus.parentWithoutId = true);
        }

        @Test
        @DisplayName("a landed write whose transitive dependent fails to rebuild returns and keeps the written type's previous generation too")
        void aFailingDependentPublishesNothing() {
            this.assertAFailedWriteKeepsEverything(() -> this.corpus.failing = LinkedGrandchild.class);
        }

        @Test
        @DisplayName("a landed write whose rebuild meets a plain link naming no row returns, publishes nothing and reports the failure on every covered type")
        void aDanglingLinkPublishesNothing() {
            this.assertAFailedWriteKeepsEverything(() -> this.corpus.children.put("c2", "p9"));
        }

        @Test
        @DisplayName("an upsert with a row whose plain link names no row is refused whole before it is written, and rebuilds nothing")
        void anUpsertNamingAMissingRowIsRefused() {
            Repository<LinkedChild> children = this.repository(LinkedChild.class);
            LinkedChild child = children.getRows().getFirst();
            int childReads = this.corpus.readsOf(LinkedChild.class);

            JpaException thrown = assertThrows(
                JpaException.class,
                () -> this.session.write(WriteRequest.upsert(LinkedChild.class, List.of(child("c2", "p1"), child("c3", "p9"))))
            );

            assertThat(thrown.getMessage(), equalTo("Field 'parent' of '" + LinkedChild.class.getName() + "' names 'p9', which no row carries"));
            assertThat(this.corpus.children.keySet(), contains("c1"));
            assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(childReads));
            assertThat(children.getState(), equalTo(HydrationState.CURRENT));
            assertThat(children.getRows(), hasSize(1));
            assertThat(children.getRows().getFirst(), sameInstance(child));
        }

        private void assertAFailedWriteKeepsEverything(@NotNull Runnable breakTheSource) {
            Repository<LinkedParent> parents = this.repository(LinkedParent.class);
            Repository<LinkedChild> children = this.repository(LinkedChild.class);
            Repository<LinkedGrandchild> grandchildren = this.repository(LinkedGrandchild.class);
            LinkedParent parent = parents.getRows().getFirst();
            LinkedChild child = children.getRows().getFirst();
            LinkedGrandchild grandchild = grandchildren.getRows().getFirst();
            breakTheSource.run();

            this.session.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno"))));

            assertThat(this.corpus.parents.get("p1"), equalTo("uno"));
            assertThat(parents.getState(), equalTo(HydrationState.DEGRADED));
            assertThat(children.getState(), equalTo(HydrationState.DEGRADED));
            assertThat(grandchildren.getState(), equalTo(HydrationState.DEGRADED));
            assertThat(parents.getRows().getFirst(), sameInstance(parent));
            assertThat(parents.getRows().getFirst().getName(), equalTo("one"));
            assertThat(children.getRows().getFirst(), sameInstance(child));
            assertThat(children.getRows().getFirst().getParent(), sameInstance(parent));
            assertThat(grandchildren.getRows().getFirst(), sameInstance(grandchild));
        }

        @Test
        @DisplayName("a write the origin refuses still throws, and rebuilds nothing")
        void aWriteTheOriginRefusesStillThrows() {
            Repository<LinkedParent> parents = this.repository(LinkedParent.class);
            LinkedParent parent = parents.getRows().getFirst();
            int parentReads = this.corpus.readsOf(LinkedParent.class);
            int childReads = this.corpus.readsOf(LinkedChild.class);
            int grandchildReads = this.corpus.readsOf(LinkedGrandchild.class);
            this.corpus.refusing = true;

            assertThrows(JpaException.class, () -> this.session.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno")))));

            assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(parentReads));
            assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(childReads));
            assertThat(this.corpus.readsOf(LinkedGrandchild.class), equalTo(grandchildReads));
            assertThat(parents.getState(), equalTo(HydrationState.CURRENT));
            assertThat(parents.getRows().getFirst(), sameInstance(parent));
            assertThat(this.corpus.parents.get("p1"), equalTo("one"));
        }

        @Test
        @DisplayName("a write naming no rows writes and rebuilds nothing")
        void anEmptyWriteRebuildsNothing() {
            int reads = this.corpus.readsOf(LinkedParent.class);

            this.session.write(WriteRequest.upsert(LinkedParent.class, List.of()));

            assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(reads));
        }

        @Test
        @DisplayName("two writes never rebuild at the same time")
        void rebuildsRunOneAtATime() throws Exception {
            this.corpus.maxInFlight.set(0);
            this.corpus.gate = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread first = new Thread(() -> this.writeCatching(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno"))), failure));
            first.start();
            assertTrue(this.corpus.parked.await(10, TimeUnit.SECONDS), "the first rebuild never reached the gate");

            Thread second = new Thread(() -> this.writeCatching(WriteRequest.upsert(LinkedGrandchild.class, List.of(grandchild("g2", "c1"))), failure));
            second.start();

            // The second write applies to the source, then waits for the session. Give it until it is
            // parked on the session or done, so a rebuild that did not wait would be caught reading.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (second.isAlive() && second.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline)
                Thread.onSpinWait();

            this.corpus.gate.countDown();
            first.join(10_000);
            second.join(10_000);

            assertThat(failure.get(), nullValue());
            assertThat(this.corpus.maxInFlight.get(), equalTo(1));
            assertThat(this.repository(LinkedGrandchild.class).getRows(), hasSize(2));
        }

        private void writeCatching(@NotNull WriteRequest<?> request, @NotNull AtomicReference<Throwable> failure) {
            try {
                this.session.write(request);
            } catch (Throwable throwable) {
                failure.compareAndSet(null, throwable);
            }
        }

    }

    @Test
    @DisplayName("a generation is visible only once it is held, and its links resolve before that")
    void nothingIsPublishedBeforeItIsHeld() {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.children.put("c1", "p1");
        LinkedParent parent = parent("p1", "one");

        JpaRepository<LinkedChild> repository = new JpaRepository<>(LinkedChild.class, Duration.ZERO);
        ConcurrentList<LinkedChild> read = repository.hydrate(corpus);

        assertThat(read, hasSize(1));
        assertThat(repository.getRows(), empty());
        assertThat(repository.getState(), equalTo(HydrationState.HYDRATING));

        repository.link(read, target -> JpaModel.keyed(LinkedParent.class, List.of(parent)));

        assertThat(read.getFirst().getParent(), sameInstance(parent));
        assertThat(repository.getRows(), empty());

        repository.hold(read);

        assertThat(repository.getRows().getFirst().getParent(), sameInstance(parent));
        assertThat(repository.getState(), equalTo(HydrationState.CURRENT));
    }

    @Test
    @DisplayName("a failure with nothing published is FAILED and serves nothing; with a generation it is DEGRADED and serves it")
    void failureStatesFollowWhatIsPublished() {
        JpaRepository<LinkedParent> repository = new JpaRepository<>(LinkedParent.class, Duration.ZERO);

        repository.fail();
        assertThat(repository.getState(), equalTo(HydrationState.FAILED));
        assertThrows(JpaException.class, repository::getRows);

        ConcurrentList<LinkedParent> rows = Concurrent.newList(parent("p1", "one"));
        repository.hold(rows);
        repository.fail();

        assertThat(repository.getState(), equalTo(HydrationState.DEGRADED));
        assertThat(repository.getRows(), hasSize(1));
    }

    @Test
    @DisplayName("two types linking to each other connect, and a write to one rebuilds both together")
    void aCycleRebuildsTogether() {
        CycleCorpus corpus = new CycleCorpus();
        SessionManager manager = new SessionManager();

        try {
            JpaSession session = manager.connect(new JpaConfig(JpaModel.resolveModels(CycleA.class), corpus));
            CycleA a = session.getRepository(CycleA.class).orElseThrow().getRows().getFirst();
            CycleB b = session.getRepository(CycleB.class).orElseThrow().getRows().getFirst();
            assertThat(a.getPartner(), sameInstance(b));
            assertThat(b.getPartner(), sameInstance(a));

            CycleA written = new CycleA();
            written.setId("a1");
            written.setPartnerId("b1");
            session.write(WriteRequest.upsert(CycleA.class, List.of(written)));

            CycleA heldA = session.getRepository(CycleA.class).orElseThrow().getRows().getFirst();
            CycleB heldB = session.getRepository(CycleB.class).orElseThrow().getRows().getFirst();
            assertThat(corpus.reads.get(), equalTo(4));
            assertThat(heldA.getPartner(), sameInstance(heldB));
            assertThat(heldB.getPartner(), sameInstance(heldA));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("a link declared as a supertype resolves to the registered subtype, and a write to that subtype rebuilds the linking type")
    void aLinkToASupertypeFollowsItsRegisteredSubtype() {
        SubtypeCorpus corpus = new SubtypeCorpus();
        SessionManager manager = new SessionManager();

        try {
            JpaSession session = manager.connect(new JpaConfig(models(SubtypeLinker.class, SubtypeRow.class), corpus));
            SubtypeRow row = session.getRepository(SubtypeRow.class).orElseThrow().getRows().getFirst();
            SubtypeLinker linker = session.getRepository(SubtypeLinker.class).orElseThrow().getRows().getFirst();
            assertThat(linker.getRow(), sameInstance(row));

            session.write(WriteRequest.upsert(SubtypeRow.class, List.of(SubtypeCorpus.row("r1", "renamed"))));

            SubtypeRow heldRow = session.getRepository(SubtypeRow.class).orElseThrow().getRows().getFirst();
            SubtypeLinker heldLinker = session.getRepository(SubtypeLinker.class).orElseThrow().getRows().getFirst();
            assertThat(heldRow.getName(), equalTo("renamed"));
            assertThat(heldLinker.getRow(), sameInstance(heldRow));
            assertThat(corpus.linkerReads.get(), equalTo(2));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("two new rows of different types naming each other through plain links are refused, whichever is written first")
    void aNewCycleAcrossTypesIsRefused() {
        CycleCorpus corpus = new CycleCorpus();
        SessionManager manager = new SessionManager();

        try {
            JpaSession session = manager.connect(new JpaConfig(JpaModel.resolveModels(CycleA.class), corpus));
            CycleA a = new CycleA();
            a.setId("a2");
            a.setPartnerId("b2");
            CycleB b = new CycleB();
            b.setId("b2");
            b.setPartnerId("a2");

            JpaException aFirst = assertThrows(JpaException.class, () -> session.write(WriteRequest.upsert(CycleA.class, List.of(a))));
            JpaException bFirst = assertThrows(JpaException.class, () -> session.write(WriteRequest.upsert(CycleB.class, List.of(b))));

            assertThat(aFirst.getMessage(), equalTo("Field 'partner' of '" + CycleA.class.getName() + "' names 'b2', which no row carries"));
            assertThat(bFirst.getMessage(), equalTo("Field 'partner' of '" + CycleB.class.getName() + "' names 'a2', which no row carries"));
            assertThat(corpus.writes.get(), equalTo(0));
            assertThat(corpus.reads.get(), equalTo(2));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("a plain link naming no row refuses the connect, naming the type and the id")
    void aDanglingLinkRefusesTheConnect() {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.parents.put("p1", "one");
        corpus.children.put("c1", "p1");
        corpus.children.put("c2", "p9");
        SessionManager manager = new SessionManager();

        try {
            JpaException thrown = assertThrows(
                JpaException.class,
                () -> manager.connect(new JpaConfig(models(LinkedParent.class, LinkedChild.class), corpus))
            );

            assertThat(thrown.getMessage(), equalTo("Field 'parent' of '" + LinkedChild.class.getName() + "' names 'p9', which no row carries"));
            assertThat(manager.isActive(), is(false));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("an Optional link holds the row its id names, and empty for an absent id or one naming no row, on connect and on write")
    void anOptionalLinkHoldsEmptyForAMiss() {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.parents.put("p1", "one");
        corpus.strays.put("s1", Optional.of("p1"));
        corpus.strays.put("s2", Optional.of("p9"));
        corpus.strays.put("s3", Optional.empty());
        SessionManager manager = new SessionManager();

        try {
            JpaSession session = manager.connect(new JpaConfig(models(LinkedParent.class, LinkedStray.class), corpus));
            LinkedParent parent = session.getRepository(LinkedParent.class).orElseThrow().getRows().getFirst();
            ConcurrentMap<String, LinkedStray> strays = JpaModel.keyed(LinkedStray.class, session.getRepository(LinkedStray.class).orElseThrow().getRows());

            assertThat(strays.get("s1").getParent().orElseThrow(), sameInstance(parent));
            assertThat(strays.get("s2").getParent(), equalTo(Optional.empty()));
            assertThat(strays.get("s3").getParent(), equalTo(Optional.empty()));

            session.write(WriteRequest.upsert(LinkedStray.class, List.of(stray("s4", "p9"))));

            LinkedStray written = JpaModel.keyed(LinkedStray.class, session.getRepository(LinkedStray.class).orElseThrow().getRows()).get("s4");
            assertThat(corpus.strays.get("s4"), equalTo(Optional.of("p9")));
            assertThat(written.getParent(), equalTo(Optional.empty()));
            assertThat(session.getRepository(LinkedStray.class).orElseThrow().getState(), equalTo(HydrationState.CURRENT));
        } finally {
            manager.shutdown();
        }
    }

    @Test
    @DisplayName("an upsert linking to a row the same request adds is written, and one linking to a row nothing holds is refused")
    void anUpsertLinksAgainstItsOwnRows() {
        LinkedCorpus corpus = new LinkedCorpus();
        corpus.siblings.put("s1", "s1");
        SessionManager manager = new SessionManager();

        try {
            JpaSession session = manager.connect(new JpaConfig(models(LinkedSibling.class), corpus));

            session.write(WriteRequest.upsert(LinkedSibling.class, List.of(sibling("s2", "s3"), sibling("s3", "s2"))));

            Repository<LinkedSibling> siblings = session.getRepository(LinkedSibling.class).orElseThrow();
            ConcurrentMap<String, LinkedSibling> held = JpaModel.keyed(LinkedSibling.class, siblings.getRows());
            assertThat(siblings.getState(), equalTo(HydrationState.CURRENT));
            assertThat(held.get("s2").getSibling(), sameInstance(held.get("s3")));
            assertThat(held.get("s3").getSibling(), sameInstance(held.get("s2")));

            assertThrows(JpaException.class, () -> session.write(WriteRequest.upsert(LinkedSibling.class, List.of(sibling("s4", "s5")))));
            assertThat(corpus.siblings.containsKey("s4"), is(false));
        } finally {
            manager.shutdown();
        }
    }

    /**
     * A writable source over one {@link CycleA} and one {@link CycleB} naming each other, answering
     * fresh instances on every read and counting the writes that reach it without applying them.
     */
    private static final class CycleCorpus implements Source.Writable {

        private final @NotNull AtomicInteger reads = new AtomicInteger();
        private final @NotNull AtomicInteger writes = new AtomicInteger();

        @Override
        @SuppressWarnings("unchecked")
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            this.reads.incrementAndGet();

            if (type == CycleA.class) {
                CycleA a = new CycleA();
                a.setId("a1");
                a.setPartnerId("b1");
                return (ConcurrentList<T>) Concurrent.newList(a);
            }

            CycleB b = new CycleB();
            b.setId("b1");
            b.setPartnerId("a1");
            return (ConcurrentList<T>) Concurrent.newList(b);
        }

        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) {
            this.writes.incrementAndGet();
        }

    }

    /**
     * A writable source over {@link SubtypeRow} names and one {@link SubtypeLinker} naming the row
     * {@code r1}, answering fresh instances on every read and counting the linker's reads.
     */
    private static final class SubtypeCorpus implements Source.Writable {

        private final @NotNull ConcurrentMap<String, String> names = Concurrent.newLinkedMap();
        private final @NotNull AtomicInteger linkerReads = new AtomicInteger();

        private SubtypeCorpus() {
            this.names.put("r1", "one");
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            ConcurrentList<JpaModel> rows = Concurrent.newList();

            if (type == SubtypeRow.class)
                this.names.forEach((id, name) -> rows.add(row(id, name)));
            else {
                this.linkerReads.incrementAndGet();
                SubtypeLinker linker = new SubtypeLinker();
                linker.setId("l1");
                linker.setRowId("r1");
                rows.add(linker);
            }

            return (ConcurrentList<T>) rows;
        }

        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) {
            request.rows().forEach(row -> {
                if (row instanceof SubtypeRow written)
                    this.names.put(written.getId(), written.getName());
            });
        }

        /**
         * Builds a subtype row.
         *
         * @param id the row's id
         * @param name the row's name
         * @return the row
         */
        private static @NotNull SubtypeRow row(@NotNull String id, @NotNull String name) {
            SubtypeRow row = new SubtypeRow();
            row.setId(id);
            row.setName(name);
            return row;
        }

    }

}

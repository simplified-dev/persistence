package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.cadence.CadencedCorpus;
import dev.simplified.persistence.cadence.CadencedDependent;
import dev.simplified.persistence.cadence.CadencedRow;
import dev.simplified.persistence.cadence.OffTickRow;
import dev.simplified.persistence.cadence.StaleRow;
import dev.simplified.persistence.cadence.TickRow;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.WriteRequest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a session's background cadence behaves: a cadenced type is re-read with no write and takes the
 * types linking into it along, a failed tick degrades what it covered and is logged naming it, and a
 * later tick recovers it, as it does after a landed write whose rebuild failed, a generation standing
 * past its stale threshold reports it on read while one whose cadence falls between two ticks never
 * does, and a shut-down session stops reading and is released.
 *
 * <p>Against a source that fingerprints its types, a tick reads only what moved: an unmoved due type
 * is confirmed rather than read however often the cadence ticks, a moved one is rebuilt with its
 * dependents, a {@link HydrationState#DEGRADED} one is read whatever its fingerprint says, and a
 * change landing while the session connects or a write the session applies is read at the next tick.
 *
 * <p>Every wait is bounded and polls for the state it needs, so none of the cases rests on a tick
 * landing inside a fixed sleep.
 */
class JpaSessionCadenceTest {

    private CadencedCorpus corpus;
    private SessionManager sessionManager;

    /**
     * Every event the session's logger emitted during the case.
     */
    private final @NotNull ConcurrentList<LogEvent> logged = Concurrent.newList();

    /**
     * Collects what the session logs into {@link #logged}, attached to its logger for each case.
     */
    private final @NotNull AbstractAppender capture = new AbstractAppender("JpaSessionCadenceTest", null, null, true, Property.EMPTY_ARRAY) {
        @Override
        public void append(@NotNull LogEvent event) {
            JpaSessionCadenceTest.this.logged.add(event.toImmutable());
        }
    };

    /**
     * Reaches the logger {@link JpaSession} writes to.
     *
     * @return the session's logger
     */
    private static @NotNull Logger sessionLogger() {
        return (Logger) LogManager.getLogger(JpaSession.class);
    }

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    /**
     * Polls a condition until it holds, failing once ten seconds pass without it.
     *
     * @param condition what the case waits for
     * @param failure the message a timeout fails with
     */
    private static void awaitUntil(@NotNull BooleanSupplier condition, @NotNull String failure) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, failure);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    @BeforeEach
    void setUp() {
        this.corpus = new CadencedCorpus();
        this.sessionManager = new SessionManager();
        this.capture.start();
        sessionLogger().addAppender(this.capture);
    }

    @AfterEach
    void shutdown() {
        this.sessionManager.shutdown();
        sessionLogger().removeAppender(this.capture);
        this.capture.stop();
    }

    private @NotNull JpaSession connect(@NotNull Class<?>... types) {
        return this.sessionManager.connect(new JpaConfig(models(types), this.corpus));
    }

    /**
     * Has the source answer a fingerprint for the cadenced row and its dependent, so every tick finds
     * both unmoved until a case moves one.
     */
    private void fingerprintBoth() {
        this.corpus.fingerprints.put(CadencedRow.class, "row-one");
        this.corpus.fingerprints.put(CadencedDependent.class, "dependent-one");
    }

    @Test
    @DisplayName("a cadenced type is re-read with no write, so a change at the origin reaches its reader")
    void aCadencedTypeIsReReadWithNoWrite() {
        Repository<CadencedRow> rows = this.connect(CadencedRow.class, CadencedDependent.class)
            .getRepository(CadencedRow.class)
            .orElseThrow();
        CadencedRow connected = rows.getRows().getFirst();
        Instant connectedAt = rows.getHydratedAt();
        int reads = this.corpus.readsOf(CadencedRow.class);
        assertThat(connected.getName(), equalTo("one"));

        this.corpus.name = "two";

        awaitUntil(() -> rows.getRows().getFirst().getName().equals("two"), "the cadence never re-read the changed row");
        assertThat(this.corpus.readsOf(CadencedRow.class), greaterThan(reads));
        assertThat(rows.getRows().getFirst(), not(sameInstance(connected)));
        assertThat(rows.getHydratedAt(), greaterThan(connectedAt));
    }

    @Test
    @DisplayName("a type linking into a cadenced one is re-read and relinked with it")
    void aDependentIsRelinkedWithItsTarget() {
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        Repository<CadencedDependent> dependents = session.getRepository(CadencedDependent.class).orElseThrow();
        CadencedRow connected = dependents.getRows().getFirst().getRow();
        int reads = this.corpus.readsOf(CadencedDependent.class);
        assertThat(connected, notNullValue());

        awaitUntil(() -> this.corpus.readsOf(CadencedDependent.class) > reads, "the dependent was never re-read");

        // Shutting down waits for the rebuild in flight and stops the next, so the two generations
        // read below were published by the same tick.
        this.sessionManager.shutdown();

        CadencedRow held = rows.getRows().getFirst();
        assertThat(held, not(sameInstance(connected)));
        assertThat(dependents.getRows().getFirst().getRow(), sameInstance(held));
    }

    @Test
    @DisplayName("a failed tick leaves every type it covered DEGRADED on its rows and logs them, and a later tick restores CURRENT")
    void aFailedTickDegradesAndALaterTickRecovers() {
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        Repository<CadencedDependent> dependents = session.getRepository(CadencedDependent.class).orElseThrow();

        this.corpus.failing = CadencedRow.class;
        awaitUntil(
            () -> rows.getState() == HydrationState.DEGRADED && dependents.getState() == HydrationState.DEGRADED,
            "a failing tick never left the covered types DEGRADED"
        );

        awaitUntil(() -> !this.logged.isEmpty(), "the failed tick was never logged");
        LogEvent failure = this.logged.getFirst();
        assertThat(failure.getLevel(), equalTo(Level.ERROR));
        assertThat(failure.getMessage().getFormattedMessage(), allOf(
            containsString(CadencedRow.class.getName()),
            containsString(CadencedDependent.class.getName())
        ));
        assertThat(failure.getThrown(), instanceOf(JpaException.class));

        // Nothing publishes while the read fails, so the rows served now are the ones served
        // throughout, and the cadence keeps asking.
        CadencedRow served = rows.getRows().getFirst();
        Instant servedAt = rows.getHydratedAt();
        int failures = this.corpus.failuresOf(CadencedRow.class);
        awaitUntil(() -> this.corpus.failuresOf(CadencedRow.class) > failures, "the cadence stopped after a failed tick");
        assertThat(rows.getRows().getFirst(), sameInstance(served));
        assertThat(rows.getHydratedAt(), equalTo(servedAt));

        this.corpus.failing = null;
        awaitUntil(
            () -> rows.getState() == HydrationState.CURRENT && dependents.getState() == HydrationState.CURRENT,
            "no later tick restored CURRENT"
        );
        assertThat(rows.getHydratedAt(), greaterThan(servedAt));
        assertThat(rows.getRows().getFirst(), not(sameInstance(served)));
    }

    @Test
    @DisplayName("a generation past its stale threshold with no tick due reports STALE and still serves its rows")
    void aGenerationPastItsThresholdIsStale() {
        Repository<StaleRow> rows = this.connect(StaleRow.class)
            .getRepository(StaleRow.class)
            .orElseThrow();
        Instant connectedAt = rows.getHydratedAt();

        awaitUntil(() -> rows.getState() == HydrationState.STALE, "the generation never reported STALE");

        // No tick came due, so the generation standing is the one the connect published.
        assertThat(this.corpus.readsOf(StaleRow.class), equalTo(1));
        assertThat(rows.getHydratedAt(), equalTo(connectedAt));
        assertThat(rows.getRows(), hasSize(1));
        assertThat(rows.getRows().getFirst().getName(), equalTo("one"));
    }

    @Test
    @DisplayName("a type whose cadence falls between one tick and the next never reports STALE while the ticks keep up")
    void anOffTickCadenceIsNeverStale() {
        // OffTickRow is not yet due at the first tick, so the second picks it up, once twice the tick
        // and a TickRow read have passed - later than twice its own cadence. Registered first, it is
        // the first type that tick reads.
        Repository<OffTickRow> rows = this.connect(OffTickRow.class, TickRow.class)
            .getRepository(OffTickRow.class)
            .orElseThrow();
        int reads = this.corpus.readsOf(OffTickRow.class);

        awaitUntil(() -> {
            assertThat(rows.getState(), not(equalTo(HydrationState.STALE)));
            return this.corpus.readsOf(OffTickRow.class) >= reads + 2;
        }, "the off-tick type was never rebuilt twice");
    }

    @Test
    @DisplayName("a due type whose fingerprint has not moved is not read, and keeps the generation it was published")
    void anUnmovedDueTypeIsNotRead() {
        this.fingerprintBoth();
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        CadencedRow connected = rows.getRows().getFirst();
        Instant connectedAt = rows.getHydratedAt();
        int asks = this.corpus.asks();

        // Ticks run one at a time, so the second ask after the connect follows a whole tick.
        awaitUntil(() -> this.corpus.asks() >= asks + 2, "no tick asked the source for fingerprints");

        assertThat(this.corpus.readsOf(CadencedRow.class), equalTo(1));
        assertThat(this.corpus.readsOf(CadencedDependent.class), equalTo(1));
        assertThat(rows.getRows().getFirst(), sameInstance(connected));
        assertThat(rows.getHydratedAt(), equalTo(connectedAt));
        assertThat(rows.getState(), equalTo(HydrationState.CURRENT));
    }

    @Test
    @DisplayName("ticking repeatedly against an unmoved origin reads nothing, and the checks keep the generation from reporting STALE")
    void repeatedTicksAgainstAnUnmovedOriginReadNothing() {
        this.fingerprintBoth();
        Repository<CadencedRow> rows = this.connect(CadencedRow.class, CadencedDependent.class)
            .getRepository(CadencedRow.class)
            .orElseThrow();
        int asks = this.corpus.asks();

        // Five ticks outlast the row's stale threshold several times over, so a check that did not
        // restart it would show here as STALE.
        awaitUntil(() -> {
            assertThat(rows.getState(), equalTo(HydrationState.CURRENT));
            return this.corpus.asks() >= asks + 5;
        }, "the cadence stopped asking");

        assertThat(this.corpus.readsOf(CadencedRow.class), equalTo(1));
        assertThat(this.corpus.readsOf(CadencedDependent.class), equalTo(1));
    }

    @Test
    @DisplayName("a due type whose fingerprint moved is rebuilt with every type linking into it, once")
    void aMovedTypeIsRebuiltWithItsDependents() {
        this.fingerprintBoth();
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        Repository<CadencedDependent> dependents = session.getRepository(CadencedDependent.class).orElseThrow();
        Instant connectedAt = rows.getHydratedAt();

        this.corpus.name = "two";
        this.corpus.fingerprints.put(CadencedRow.class, "row-two");

        awaitUntil(() -> rows.getRows().getFirst().getName().equals("two"), "the moved type was never re-read");

        // Shutting down waits for the rebuild in flight, so the generations read below are the ones
        // that rebuild published, and the counts are final.
        this.sessionManager.shutdown();

        CadencedRow held = rows.getRows().getFirst();
        assertThat(dependents.getRows().getFirst().getRow(), sameInstance(held));
        assertThat(rows.getHydratedAt(), greaterThan(connectedAt));
        assertThat(this.corpus.readsOf(CadencedRow.class), equalTo(2));
        assertThat(this.corpus.readsOf(CadencedDependent.class), equalTo(2));
    }

    @Test
    @DisplayName("a source that fingerprints nothing has every due type read at every tick")
    void aSourceThatFingerprintsNothingRebuildsEveryDueType() {
        this.connect(CadencedRow.class, CadencedDependent.class);
        int reads = this.corpus.readsOf(CadencedRow.class);
        int dependentReads = this.corpus.readsOf(CadencedDependent.class);
        int asks = this.corpus.asks();

        awaitUntil(() -> this.corpus.readsOf(CadencedRow.class) >= reads + 2, "the unfingerprinted type was not re-read");

        assertThat(this.corpus.name, equalTo("one"));
        assertThat(this.corpus.asks(), greaterThan(asks));
        assertThat(this.corpus.readsOf(CadencedDependent.class), greaterThan(dependentReads));
    }

    @Test
    @DisplayName("a DEGRADED type is read at every tick even while its fingerprint matches the one its rows were read under")
    void aDegradedTypeIsReadEvenWhenUnmoved() {
        this.fingerprintBoth();
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        Repository<CadencedDependent> dependents = session.getRepository(CadencedDependent.class).orElseThrow();

        this.corpus.failing = CadencedRow.class;
        this.corpus.fingerprints.put(CadencedRow.class, "row-two");
        awaitUntil(() -> rows.getState() == HydrationState.DEGRADED, "the moved type's failed rebuild never left it DEGRADED");

        // The source answers the fingerprint the held rows were read under again, so only the
        // failed rebuild sends the type back to it. A tick already past its ask may fail once more
        // under the old answer, so the second failure from here is the one asked under this one.
        this.corpus.fingerprints.put(CadencedRow.class, "row-one");
        int failures = this.corpus.failuresOf(CadencedRow.class);
        awaitUntil(() -> this.corpus.failuresOf(CadencedRow.class) >= failures + 2, "an unmoved DEGRADED type was not read again");

        this.corpus.failing = null;
        awaitUntil(
            () -> rows.getState() == HydrationState.CURRENT && dependents.getState() == HydrationState.CURRENT,
            "no later tick restored CURRENT"
        );
        assertThat(this.corpus.readsOf(CadencedRow.class), equalTo(2));
    }

    @Test
    @DisplayName("a change landing while the session connects is read at the next tick")
    void aChangeDuringConnectIsPickedUpByTheNextTick() {
        this.fingerprintBoth();

        // The change lands once the connect has read the row and before it reads the dependent, so
        // a fingerprint asked after the read would already name it and the row would never be read
        // again.
        this.corpus.afterRead = type -> {
            if (type != CadencedRow.class)
                return;

            this.corpus.afterRead = null;
            this.corpus.name = "two";
            this.corpus.fingerprints.put(CadencedRow.class, "row-two");
        };

        Repository<CadencedRow> rows = this.connect(CadencedRow.class, CadencedDependent.class)
            .getRepository(CadencedRow.class)
            .orElseThrow();
        assertThat(rows.getRows().getFirst().getName(), equalTo("one"));

        awaitUntil(() -> rows.getRows().getFirst().getName().equals("two"), "the change made during the connect never reached the session");
    }

    @Test
    @DisplayName("a write leaves every type it rebuilt to be read once more at the next tick, whatever the fingerprint says")
    void aWriteIsReadAgainAtTheNextTick() {
        this.fingerprintBoth();
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        CadencedRow written = new CadencedRow();
        written.setId("r1");
        written.setName("two");

        session.write(WriteRequest.upsert(CadencedRow.class, List.of(written)));
        assertThat(rows.getRows().getFirst().getName(), equalTo("two"));
        int reads = this.corpus.readsOf(CadencedRow.class);
        int dependentReads = this.corpus.readsOf(CadencedDependent.class);

        // The source never moves the fingerprint, the way a catalogue lags the commit, so only the
        // forgotten fingerprint sends the written type and its dependent back to it.
        awaitUntil(() -> this.corpus.readsOf(CadencedRow.class) > reads, "the written type was not read again at its next tick");

        // That read records the answer again, so the ticks after it read nothing.
        int asks = this.corpus.asks();
        awaitUntil(() -> this.corpus.asks() >= asks + 2, "the cadence stopped asking");
        assertThat(this.corpus.readsOf(CadencedRow.class), equalTo(reads + 1));
        assertThat(this.corpus.readsOf(CadencedDependent.class), equalTo(dependentReads + 1));
    }

    @Test
    @DisplayName("a landed write whose rebuild fails returns and is logged naming every type it covered, and a later tick serves the write")
    void aLandedWriteWhoseRebuildFailsIsServedByALaterTick() {
        this.fingerprintBoth();
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        Repository<CadencedRow> rows = session.getRepository(CadencedRow.class).orElseThrow();
        Repository<CadencedDependent> dependents = session.getRepository(CadencedDependent.class).orElseThrow();
        CadencedRow written = new CadencedRow();
        written.setId("r1");
        written.setName("two");

        this.corpus.failing = CadencedRow.class;
        session.write(WriteRequest.upsert(CadencedRow.class, List.of(written)));

        // A tick while the read still fails passes through REFRESHING and fails again, so the
        // covered types are waited for rather than read once.
        assertThat(this.corpus.name, equalTo("two"));
        awaitUntil(
            () -> rows.getState() == HydrationState.DEGRADED && dependents.getState() == HydrationState.DEGRADED,
            "the failed rebuild never left the covered types DEGRADED"
        );
        assertThat(rows.getRows().getFirst().getName(), equalTo("one"));

        awaitUntil(
            () -> this.logged.stream().anyMatch(event -> event.getMessage().getFormattedMessage().startsWith("A write to")),
            "the failed rebuild after the write was never logged"
        );
        LogEvent failure = this.logged.stream()
            .filter(event -> event.getMessage().getFormattedMessage().startsWith("A write to"))
            .findFirst()
            .orElseThrow();
        assertThat(failure.getLevel(), equalTo(Level.ERROR));
        assertThat(failure.getMessage().getFormattedMessage(), allOf(
            containsString(CadencedRow.class.getName()),
            containsString(CadencedDependent.class.getName())
        ));
        assertThat(failure.getThrown(), instanceOf(JpaException.class));

        this.corpus.failing = null;
        awaitUntil(
            () -> rows.getState() == HydrationState.CURRENT && dependents.getState() == HydrationState.CURRENT,
            "no later tick restored CURRENT"
        );
        assertThat(rows.getRows().getFirst().getName(), equalTo("two"));
    }

    @Test
    @DisplayName("a shut-down session reads nothing more, and nothing keeps it reachable")
    void aShutDownSessionIsReleased() throws InterruptedException {
        WeakReference<JpaSession> released = this.connectTickAndShutDown();
        int reads = this.corpus.readsOf(CadencedRow.class);

        for (int attempt = 0; attempt < 100 && released.get() != null; attempt++) {
            System.gc();
            Thread.sleep(50);
        }

        // A tick holds its session, so once the session is collected no tick can read again; every
        // read after the shutdown would show in the count.
        assertThat("the session stayed reachable after shutdown", released.get(), nullValue());
        assertThat(this.corpus.readsOf(CadencedRow.class), equalTo(reads));
    }

    /**
     * Connects a cadenced session, waits for it to tick, and shuts it down, holding the session only
     * weakly from here on.
     *
     * @return a weak reference to the shut-down session
     */
    private @NotNull WeakReference<JpaSession> connectTickAndShutDown() {
        JpaSession session = this.connect(CadencedRow.class, CadencedDependent.class);
        int reads = this.corpus.readsOf(CadencedRow.class);

        awaitUntil(() -> this.corpus.readsOf(CadencedRow.class) > reads, "the cadence never ticked");
        this.sessionManager.shutdown();

        assertThat(session.isActive(), equalTo(false));
        return new WeakReference<>(session);
    }

}

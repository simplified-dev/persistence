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
 * later tick recovers it, a generation standing past its stale threshold reports it on read while one
 * whose cadence falls between two ticks never does, and a shut-down session stops reading and is
 * released.
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

package dev.simplified.persistence.cadence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * A source held in memory over the cadence models, answering fresh instances on every read the way a
 * parsed document does.
 *
 * <p>It holds one row of each type, all carrying the same name, which can be changed without a write
 * the way a document changes at its origin, or through one. It counts the reads it answered and the
 * reads it failed, per type, and can be told to fail one type's read. A {@link TickRow} read takes
 * {@link #TICK_ROW_READ_COST}, the way a read from a slow origin does.
 *
 * <p>It fingerprints nothing until a case says what each type's fingerprint is, and counts how often
 * it was asked.
 */
public final class CadencedCorpus implements Source.Writable {

    /**
     * How long a {@link TickRow} read takes before it answers.
     */
    public static final @NotNull Duration TICK_ROW_READ_COST = Duration.ofMillis(100);

    /**
     * The name every row answered carries.
     */
    public volatile @NotNull String name = "one";

    /**
     * The type whose read throws, or {@code null} for none.
     */
    public volatile @Nullable Class<?> failing;

    /**
     * What runs once a read has answered, given the type read, or {@code null} for nothing - the
     * way a change lands at the origin while a session is still reading.
     */
    public volatile @Nullable Consumer<Class<?>> afterRead;

    /**
     * The fingerprint answered for each type, none until a case sets one.
     */
    public final @NotNull ConcurrentMap<Class<?>, String> fingerprints = Concurrent.newMap();

    /**
     * The reads answered so far, per type.
     */
    private final @NotNull ConcurrentMap<Class<?>, AtomicInteger> reads = Concurrent.newMap();

    /**
     * The reads failed so far, per type.
     */
    private final @NotNull ConcurrentMap<Class<?>, AtomicInteger> failures = Concurrent.newMap();

    /**
     * The fingerprint asks answered so far.
     */
    private final @NotNull AtomicInteger asks = new AtomicInteger();

    /**
     * Counts the reads answered for one type.
     *
     * @param type the type asked about
     * @return how many reads of it were answered
     */
    public int readsOf(@NotNull Class<?> type) {
        return countOf(this.reads, type);
    }

    /**
     * Counts the reads failed for one type.
     *
     * @param type the type asked about
     * @return how many reads of it threw
     */
    public int failuresOf(@NotNull Class<?> type) {
        return countOf(this.failures, type);
    }

    /**
     * Counts the fingerprint asks answered.
     *
     * @return how many times a session asked for fingerprints
     */
    public int asks() {
        return this.asks.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
        if (type == this.failing) {
            this.failures.computeIfAbsent(type, key -> new AtomicInteger()).incrementAndGet();
            throw new IllegalStateException(String.format("Source down for '%s'", type.getSimpleName()));
        }

        if (type == TickRow.class)
            spend(TICK_ROW_READ_COST);

        ConcurrentList<JpaModel> rows = Concurrent.newList();

        if (type == CadencedRow.class) {
            CadencedRow row = new CadencedRow();
            row.setId("r1");
            row.setName(this.name);
            rows.add(row);
        } else if (type == CadencedDependent.class) {
            CadencedDependent dependent = new CadencedDependent();
            dependent.setId("d1");
            dependent.setRowId("r1");
            rows.add(dependent);
        } else if (type == StaleRow.class) {
            StaleRow row = new StaleRow();
            row.setId("s1");
            row.setName(this.name);
            rows.add(row);
        } else if (type == TickRow.class) {
            TickRow row = new TickRow();
            row.setId("t1");
            row.setName(this.name);
            rows.add(row);
        } else if (type == OffTickRow.class) {
            OffTickRow row = new OffTickRow();
            row.setId("o1");
            row.setName(this.name);
            rows.add(row);
        } else if (type == CheckedRow.class) {
            CheckedRow row = new CheckedRow();
            row.setId("c1");
            row.setName(this.name);
            rows.add(row);
        } else if (type == CheckedDependent.class) {
            CheckedDependent dependent = new CheckedDependent();
            dependent.setId("cd1");
            dependent.setRowId("c1");
            rows.add(dependent);
        }

        this.reads.computeIfAbsent(type, key -> new AtomicInteger()).incrementAndGet();
        Consumer<Class<?>> afterRead = this.afterRead;

        if (afterRead != null)
            afterRead.accept(type);

        return (ConcurrentList<T>) rows;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Answers the fingerprint a case set for each type asked about, and leaves the rest out.
     */
    @Override
    public @NotNull ConcurrentMap<Class<? extends JpaModel>, String> fingerprints(
        @NotNull ConcurrentList<Class<JpaModel>> types
    ) {
        this.asks.incrementAndGet();
        ConcurrentMap<Class<? extends JpaModel>, String> answered = Concurrent.newMap();

        for (Class<JpaModel> type : types) {
            String fingerprint = this.fingerprints.get(type);

            if (fingerprint != null)
                answered.put(type, fingerprint);
        }

        return answered;
    }

    /**
     * {@inheritDoc}
     *
     * <p>A {@link CadencedRow} written renames every row, and a write to any other type changes
     * nothing. The fingerprints are left alone, the way an origin's catalogue lags the commit.
     */
    @Override
    public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) {
        request.rows()
            .stream()
            .filter(CadencedRow.class::isInstance)
            .map(CadencedRow.class::cast)
            .forEach(row -> this.name = row.getName());
    }

    /**
     * Reads one type's count out of a tally.
     *
     * @param counts the tally, per type
     * @param type the type asked about
     * @return the type's count, {@code 0} when it has none
     */
    private static int countOf(@NotNull ConcurrentMap<Class<?>, AtomicInteger> counts, @NotNull Class<?> type) {
        AtomicInteger count = counts.get(type);
        return count == null ? 0 : count.get();
    }

    /**
     * Holds the reading thread for a read's cost, however often its park returns early.
     *
     * @param cost how long the read takes
     */
    private static void spend(@NotNull Duration cost) {
        long until = System.nanoTime() + cost.toNanos();

        for (long left = cost.toNanos(); left > 0; left = until - System.nanoTime())
            LockSupport.parkNanos(left);
    }

}

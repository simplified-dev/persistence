package dev.simplified.persistence.cadence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.Source;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A read-only source held in memory over the cadence models, answering fresh instances on every read
 * the way a parsed document does.
 *
 * <p>It holds one row of each type, all carrying the same name, which can be changed without a write
 * the way a document changes at its origin. It counts the reads it answered and the reads it failed,
 * per type, and can be told to fail one type's read. A {@link TickRow} read takes
 * {@link #TICK_ROW_READ_COST}, the way a read from a slow origin does.
 */
public final class CadencedCorpus implements Source {

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
     * The reads answered so far, per type.
     */
    private final @NotNull ConcurrentMap<Class<?>, AtomicInteger> reads = Concurrent.newMap();

    /**
     * The reads failed so far, per type.
     */
    private final @NotNull ConcurrentMap<Class<?>, AtomicInteger> failures = Concurrent.newMap();

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
        }

        this.reads.computeIfAbsent(type, key -> new AtomicInteger()).incrementAndGet();
        return (ConcurrentList<T>) rows;
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

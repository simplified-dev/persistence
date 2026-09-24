package dev.simplified.persistence.source;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import org.jetbrains.annotations.NotNull;

/**
 * One write against a {@link Source.Writable}.
 *
 * <p>Carries what to write. The retry ladder, the conflict accounting and any queue a deployment puts
 * in front of this are that deployment's, not this library's.
 *
 * @param type the entity class being written
 * @param operation whether the rows are being written or removed
 * @param rows the rows the operation applies to
 * @param <T> the entity type
 */
public record WriteRequest<T extends JpaModel>(
    @NotNull Class<T> type,
    @NotNull Operation operation,
    @NotNull ConcurrentList<T> rows
) {

    /**
     * Builds a request that creates or replaces the given rows.
     *
     * @param type the entity class
     * @param rows the rows to write
     * @param <T> the entity type
     * @return the upsert
     */
    public static <T extends JpaModel> @NotNull WriteRequest<T> upsert(@NotNull Class<T> type, @NotNull Iterable<T> rows) {
        return new WriteRequest<>(type, Operation.UPSERT, listed(rows));
    }

    /**
     * Builds a request that removes the given rows.
     *
     * @param type the entity class
     * @param rows the rows to remove
     * @param <T> the entity type
     * @return the delete
     */
    public static <T extends JpaModel> @NotNull WriteRequest<T> delete(@NotNull Class<T> type, @NotNull Iterable<T> rows) {
        return new WriteRequest<>(type, Operation.DELETE, listed(rows));
    }

    /**
     * Gathers the given rows into one unmodifiable list.
     *
     * @param rows the rows to gather
     * @param <T> the entity type
     * @return the rows, in iteration order
     */
    private static <T extends JpaModel> @NotNull ConcurrentList<T> listed(@NotNull Iterable<T> rows) {
        ConcurrentList<T> gathered = Concurrent.newList();
        rows.forEach(gathered::add);
        return Concurrent.newUnmodifiableList(gathered);
    }

    /**
     * What a write does to the rows it names.
     */
    public enum Operation {

        /**
         * Create the rows, replacing any the origin already holds under the same keys.
         */
        UPSERT,

        /**
         * Remove the rows from the origin.
         */
        DELETE

    }

}

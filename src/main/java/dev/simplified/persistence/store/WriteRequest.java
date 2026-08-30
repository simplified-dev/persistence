package dev.simplified.persistence.store;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * One write against a {@link Source.Writable}.
 *
 * <p>Carries what to write and, optionally, the revision the caller expects the origin to still be at.
 * A GitHub source sends that revision as a precondition and retries when the origin has moved under
 * it; a file source compares a hash; a relational source ignores it. The retry ladder, the conflict
 * accounting and any queue a deployment puts in front of this are that deployment's, not this
 * library's.
 *
 * @param type the entity class being written
 * @param operation whether the rows are being written or removed
 * @param rows the rows the operation applies to
 * @param precondition the origin revision the caller expects, or {@code null} to write unconditionally
 * @param <T> the entity type
 */
public record WriteRequest<T extends JpaModel>(
    @NotNull Class<T> type,
    @NotNull Operation operation,
    @NotNull ConcurrentList<T> rows,
    @Nullable String precondition
) {

    /**
     * Builds a request that creates or replaces the given rows.
     *
     * @param type the entity class
     * @param rows the rows to write
     * @param <T> the entity type
     * @return an unconditional upsert
     */
    public static <T extends JpaModel> @NotNull WriteRequest<T> upsert(@NotNull Class<T> type, @NotNull Iterable<T> rows) {
        return new WriteRequest<>(type, Operation.UPSERT, listed(rows), null);
    }

    /**
     * Builds a request that removes the given rows.
     *
     * @param type the entity class
     * @param rows the rows to remove
     * @param <T> the entity type
     * @return an unconditional delete
     */
    public static <T extends JpaModel> @NotNull WriteRequest<T> delete(@NotNull Class<T> type, @NotNull Iterable<T> rows) {
        return new WriteRequest<>(type, Operation.DELETE, listed(rows), null);
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
     * Returns a copy of this request that applies only while the origin is still at the given
     * revision.
     *
     * @param precondition the expected origin revision
     * @return a conditional copy of this request
     */
    public @NotNull WriteRequest<T> expecting(@NotNull String precondition) {
        return new WriteRequest<>(this.type(), this.operation(), this.rows(), precondition);
    }

    /**
     * The origin revision this write expects, empty when it applies unconditionally.
     */
    public @NotNull Optional<String> getPrecondition() {
        return Optional.ofNullable(this.precondition());
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

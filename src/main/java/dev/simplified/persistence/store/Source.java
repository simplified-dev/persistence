package dev.simplified.persistence.store;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * Where a type's rows come from, whether that is a relational table, a JSON document or anything else.
 *
 * <p>One source serves every type an origin publishes, so the type is an argument rather than a type
 * parameter. A registry of one source per model is the shape that grows with the corpus; this one does
 * not.
 *
 * <p>Reading is all a source promises. Writing is {@link Writable}, and a source that was handed no
 * write instruction simply is not one - which is how an origin a caller may read but not update is
 * expressed in the type system rather than in a document.
 *
 * @see Writable
 */
public interface Source {

    /**
     * Reads every row the origin holds for the given type.
     *
     * @param type the entity class to read
     * @param <T> the entity type
     * @return the rows, empty when the origin holds none
     * @throws JpaException if the read fails
     */
    <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException;

    /**
     * Returns the source for rows the database itself authors.
     *
     * <p>It reads nothing, because there is no external origin to read from. Every registered type has
     * a source this way, so nothing has to special-case the absence of one.
     *
     * @return a source that holds no rows
     */
    static @NotNull Source none() {
        return None.INSTANCE;
    }

    /**
     * Holder for the source of rows the database itself authors.
     */
    final class None implements Source {

        private static final @NotNull Source INSTANCE = new None();

        private None() {}

        /** {@inheritDoc} */
        @Override
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            return Concurrent.newUnmodifiableList();
        }

    }

    /**
     * The write half, for an origin a caller holds instructions to update.
     */
    interface Writable extends Source {

        /**
         * Applies one write to the origin.
         *
         * <p>Granularity is the origin's concern. A document source reads its current layers, applies
         * the request and rewrites the file; a relational source applies the rows one at a time.
         * Neither leaks into the request.
         *
         * @param request the write to apply
         * @param <T> the entity type
         * @throws JpaException if the write fails, including when the request's precondition no longer
         *         holds
         */
        <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException;

    }

}

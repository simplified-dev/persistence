package dev.simplified.persistence.source;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
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
 * @see DocumentSource
 * @see RelationalSource
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
     * The fingerprint of each given type's rows, as the origin holds them now.
     *
     * <p>Two answers carrying the same fingerprint for a type describe the same rows, so a session
     * asks before it reads and a {@link Hydration} tick skips a type whose fingerprint has not moved
     * since its held rows were read. A type the answer leaves out is one this source cannot vouch
     * for, and is read. A source that cannot fingerprint answers empty, which is the default.
     *
     * @param types the registered types asked about
     * @return the fingerprints keyed by type, empty when this source cannot fingerprint
     * @throws JpaException if the origin cannot be asked
     */
    default @NotNull ConcurrentMap<Class<? extends JpaModel>, String> fingerprints(
        @NotNull ConcurrentList<Class<JpaModel>> types
    ) throws JpaException {
        return Concurrent.newUnmodifiableMap();
    }

    /**
     * The write half, for an origin a caller holds instructions to update.
     *
     * @see DocumentSource.Writable
     * @see RelationalSource
     */
    interface Writable extends Source {

        /**
         * Applies one write to the origin.
         *
         * <p>Granularity is the origin's concern. A document source rewrites the files whose rows it
         * changes; a relational source applies the rows one at a time. Neither leaks into the
         * request.
         *
         * <p>A write here reaches the origin and nothing else. A session holding the type keeps
         * serving the rows it read, so a registered type is written through
         * {@link JpaSession#write(WriteRequest)}, which rebuilds it and every type linking into it.
         *
         * @param request the write to apply
         * @param <T> the entity type
         * @throws JpaException if the write fails, including when the origin moved under it
         */
        <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException;

    }

}

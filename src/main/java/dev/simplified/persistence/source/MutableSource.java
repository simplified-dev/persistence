package dev.simplified.persistence.source;

import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link Source} that also supports writing entities back to the external origin.
 *
 * <p>Extends the read-only {@link Source} contract with idempotent upsert and delete
 * operations. Write callers (e.g. a Phase 6 {@code IQueue<WriteRequest>} consumer)
 * dispatch the full entity to {@link #upsert(JpaModel)}, which is free to treat
 * create and update identically - the caller has already resolved the target state
 * and expects the storage to match. The returned entity reflects the persisted form,
 * so generated identifiers, merged timestamps, and server-side normalizations are
 * observable without an extra read.
 *
 * <p>No concrete {@code MutableSource} ships in Phase 4a. The GitHub-backed
 * {@code WritableRemoteJsonSource} lands in Phase 6 alongside the IQueue write path.
 *
 * @param <T> the entity type
 * @see Source
 */
public interface MutableSource<T extends JpaModel> extends Source<T> {

    /**
     * Persists the given entity to the external origin, creating it if absent or
     * replacing it if present.
     *
     * @param entity the entity to persist
     * @return the persisted entity, reflecting any origin-side normalizations
     * @throws JpaException if the write fails
     */
    @NotNull T upsert(@NotNull T entity) throws JpaException;

    /**
     * Removes the given entity from the external origin.
     *
     * @param entity the entity to delete
     * @return the deleted entity, for observability of the state transition
     * @throws JpaException if the delete fails
     */
    @NotNull T delete(@NotNull T entity) throws JpaException;

}

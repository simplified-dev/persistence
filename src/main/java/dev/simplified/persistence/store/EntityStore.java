package dev.simplified.persistence.store;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * The one contract for where a type's rows come from, whether that is a relational table, a JSON
 * document or anything else.
 *
 * <p>A store loads rows and nothing else. It advertises no capabilities and negotiates nothing with
 * its caller, so every backend answers the same single question and no caller has to ask which kind
 * it holds. A {@code null} store means the rows are authored by external SQL writes and need no
 * loading.
 *
 * <p>One abstract method, so a store that needs no state is a lambda.
 *
 * @param <T> the entity type
 */
@FunctionalInterface
public interface EntityStore<T extends JpaModel> {

    /**
     * Loads this type's rows from the origin.
     *
     * @param repository the repository requesting the rows
     * @return the loaded rows
     * @throws JpaException if the load fails
     */
    @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) throws JpaException;

    /**
     * The write half, for a store whose origin accepts changes.
     *
     * @param <T> the entity type
     */
    interface Mutable<T extends JpaModel> extends EntityStore<T> {

        /**
         * Persists the given entity to the origin, creating it if absent and replacing it if present.
         *
         * @param entity the entity to persist
         * @return the persisted entity, reflecting any origin-side normalisation
         * @throws JpaException if the write fails
         */
        @NotNull T upsert(@NotNull T entity) throws JpaException;

        /**
         * Removes the given entity from the origin.
         *
         * @param entity the entity to delete
         * @return the deleted entity
         * @throws JpaException if the delete fails
         */
        @NotNull T delete(@NotNull T entity) throws JpaException;

    }

}

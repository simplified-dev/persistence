package dev.simplified.persistence.source;

import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.collection.ConcurrentList;
import org.jetbrains.annotations.NotNull;

/**
 * Loads entity data from an external origin for persistence into the database.
 *
 * <p>Sources are used by {@link JpaRepository} to populate an embedded database
 * with data that does not originate from SQL (e.g. classpath JSON files or
 * in-memory asset collections). A {@code null} source indicates that data is
 * managed by external SQL writes and requires no loading.
 *
 * <p>Built-in sources are available via {@link #json(String)}. Custom sources
 * (e.g. asset-backed loading) can be expressed as lambdas.
 *
 * @param <T> the entity type
 */
@FunctionalInterface
public interface Source<T extends JpaModel> {

    /**
     * Loads entities from the external origin.
     *
     * @param repository the repository requesting data
     * @return the loaded entities
     * @throws JpaException if the load fails
     */
    @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) throws JpaException;

    /**
     * Returns a source that loads entities from JSON classpath resources under the given
     * base directory.
     *
     * @param resourceBase the classpath prefix (e.g. {@code "skyblock"})
     * @param <T> the entity type
     * @return a JSON-backed source
     */
    static <T extends JpaModel> @NotNull Source<T> json(@NotNull String resourceBase) {
        return new JsonSource<>(resourceBase);
    }

}

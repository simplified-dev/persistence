package dev.simplified.persistence;

import dev.simplified.persistence.exception.JpaException;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.query.Sortable;
import dev.simplified.collection.tuple.single.SingleStream;
import dev.simplified.util.time.Stopwatch;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

/**
 * Read-only query interface for cached JPA entities, extending {@link Sortable}
 * for predicate-based searching and sorting support.
 *
 * <p>Implementations query the Hibernate L2 cache, which is backed by JCache
 * regions with per-entity TTL derived from {@link CacheExpiry} annotations.
 * When cache entries expire, Hibernate transparently re-queries the database.
 *
 * @param <T> the entity type, which must implement {@link JpaModel}
 */
public interface Repository<T extends JpaModel> extends Sortable<T> {

    /**
     * The cache refresh duration derived from the {@link CacheExpiry} annotation.
     */
    @NotNull Duration getCacheDuration();

    /**
     * The {@link CacheExpiry} annotation for this repository's entity type, or {@link CacheExpiry#DEFAULT} if not annotated.
     */
    @NotNull CacheExpiry getCacheExpiry();

    /**
     * The timing snapshot of the initial data load performed during repository construction.
     */
    @NotNull Stopwatch getInitialLoad();

    /**
     * The timing snapshot of the most recent cache refresh.
     */
    @NotNull Stopwatch getLastRefresh();

    /**
     * The class type of the {@link JpaModel} associated with this repository.
     */
    @NotNull Class<T> getType();

    /**
     * Executes a Hibernate criteria query against the cached data and returns
     * the results wrapped in a {@link SingleStream}.
     *
     * @return a {@link SingleStream} containing all cached entities of type {@code T}
     * @throws JpaException if the query fails
     */
    @Override
    @NotNull SingleStream<T> stream() throws JpaException;

    /**
     * Calls {@link #stream()} and collects the results into an unmodifiable
     * {@link ConcurrentList}.
     *
     * @return an unmodifiable {@link ConcurrentList} of all cached entities
     * @throws JpaException if the underlying stream query fails
     */
    default @NotNull ConcurrentList<T> findAll() throws JpaException {
        return this.stream().collect(Concurrent.toUnmodifiableList());
    }

}

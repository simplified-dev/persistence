package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.query.IndexCache;
import dev.simplified.collection.query.Indexed;
import dev.simplified.collection.query.Sortable;
import dev.simplified.collection.tuple.single.SingleStream;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * One generation of a model's rows, and the indexes over them.
 *
 * <p>Every finder {@link Sortable} offers is written over {@link #stream()}, and every equality finder
 * reaches {@link #indexes()} first, so holding the rows is what answers all of them. A property
 * declaring {@link Indexed} is a hash probe; anything else is a scan over rows already in memory.
 * Neither reaches a database.
 *
 * <p>Where the rows came from - a JSON document, a remote corpus, a database table - is the source's
 * business and is not visible here.
 *
 * @param <T> the entity type, which must implement {@link JpaModel}
 */
public interface Repository<T extends JpaModel> extends Sortable<T> {

    /**
     * The class type of the {@link JpaModel} this repository holds.
     */
    @NotNull Class<T> getType();

    /**
     * The rows this repository holds, as one generation.
     *
     * <p>The list is unmodifiable and is replaced whole rather than mutated, so a caller holding one
     * keeps reading the generation it asked for.
     *
     * @return the held rows
     * @throws JpaException if the last hydration failed and there is nothing to serve
     */
    @NotNull ConcurrentList<T> getRows() throws JpaException;

    /**
     * The point this repository's generation has reached in its hydration lifecycle.
     */
    @NotNull HydrationState getState();

    /**
     * When the held generation was published.
     */
    @NotNull Instant getHydratedAt();

    /** {@inheritDoc} */
    @Override
    default @NotNull SingleStream<T> stream() throws JpaException {
        return SingleStream.of(this.getRows().stream());
    }

    /** {@inheritDoc} */
    @Override
    default @NotNull IndexCache<T> indexes() {
        return this.getRows().indexes();
    }

    /**
     * Returns every row this repository holds.
     *
     * @return the held rows
     * @throws JpaException if the last hydration failed and there is nothing to serve
     */
    default @NotNull ConcurrentList<T> findAll() throws JpaException {
        return this.getRows();
    }

}

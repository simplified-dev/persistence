package dev.simplified.persistence;

import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.collection.query.Sortable;
import dev.simplified.gson.PostInit;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Default {@link Repository} implementation, holding one generation of rows.
 *
 * <p>A read answers from the held generation and performs no I/O, so every finder inherited from
 * {@link Sortable} is a scan or an index probe over rows already in
 * memory. Where those rows came from - a JSON document, a GitHub corpus, a database table - is the
 * source's business and changes nothing here.
 *
 * <p>A generation is built in three steps the owning {@link JpaSession} drives:
 * {@link #hydrate(Source)} reads the rows, {@link #link(ConcurrentList, Function)} resolves their
 * links, and {@link #hold(ConcurrentList)} publishes them by one reference write. Nothing a reader can
 * reach changes before the last step, so a reader never sees a row whose links are still empty, and
 * one holding a generation never sees it change underneath them.
 *
 * @param <T> the entity type, which must implement {@link JpaModel}
 * @see Repository
 * @see JpaSession
 */
public class JpaRepository<T extends JpaModel> implements Repository<T> {

    /**
     * The entity class managed by this repository.
     */
    @Getter private final @NotNull Class<T> type;

    /**
     * How long to wait between checks against the source, or {@link Duration#ZERO} for no background
     * cadence.
     */
    private final @NotNull Duration hydrationInterval;

    /**
     * How long a generation may go unchecked before it reports {@link HydrationState#STALE}, or
     * {@link Duration#ZERO} when it never does.
     */
    private final @NotNull Duration stalenessThreshold;

    /**
     * The rows of the published generation, with every link resolved.
     */
    private volatile @NotNull ConcurrentList<T> rows = Concurrent.newUnmodifiableList();

    /**
     * The point the last hydration step left this repository's generation at.
     */
    private volatile @NotNull HydrationState state = HydrationState.UNHYDRATED;

    /**
     * When the held generation was published, {@link Instant#EPOCH} before the first.
     */
    @Getter private volatile @NotNull Instant hydratedAt = Instant.EPOCH;

    /**
     * When the source was last found to hold the held generation - at its publication, or at a tick
     * that found its origin unmoved - {@link Instant#EPOCH} before the first.
     */
    private volatile @NotNull Instant checkedAt = Instant.EPOCH;

    /**
     * Creates a repository for the given type.
     *
     * <p>No I/O runs here. The generation is built when the session reaches this type, so
     * a repository exists and answers {@link HydrationState#UNHYDRATED} before it holds anything.
     *
     * <p>A type declaring a cadence and no {@link Hydration#stale()} of its own reports
     * {@link HydrationState#STALE} once its generation has gone unchecked for its cadence plus two
     * ticks. A due type is picked up at the first tick after its cadence elapses, which comes at most
     * one tick and one rebuild later, so a generation outlives the threshold only when the rebuilds
     * take longer than a tick or the ticks stop.
     *
     * @param type the entity class
     * @param tick the interval the owning session ticks at, {@link Duration#ZERO} for no cadence
     */
    JpaRepository(@NotNull Class<T> type, @NotNull Duration tick) {
        this.type = type;
        this.hydrationInterval = intervalOf(type);

        Hydration hydration = type.getAnnotation(Hydration.class);

        if (hydration != null && hydration.stale() > 0)
            this.stalenessThreshold = Duration.of(hydration.stale(), hydration.unit().toChronoUnit());
        else if (this.hydrationInterval.isZero())
            this.stalenessThreshold = Duration.ZERO;
        else
            this.stalenessThreshold = this.hydrationInterval.plus(tick.multipliedBy(2));
    }

    /**
     * Reads the background cadence a type declares through {@link Hydration}.
     *
     * @param type the entity class to read
     * @return how long the type waits between checks, {@link Duration#ZERO} for no cadence
     */
    static @NotNull Duration intervalOf(@NotNull Class<?> type) {
        Hydration hydration = type.getAnnotation(Hydration.class);

        return hydration == null
            ? Duration.ZERO
            : Duration.of(hydration.every(), hydration.unit().toChronoUnit());
    }

    /**
     * Whether the held generation has gone unchecked for its cadence.
     *
     * @return {@code true} when a check against the source is overdue
     */
    boolean isDue() {
        return !this.hydrationInterval.isZero()
            && Duration.between(this.checkedAt, Instant.now()).compareTo(this.hydrationInterval) >= 0;
    }

    /**
     * Whether the held generation has gone unchecked past its staleness threshold.
     *
     * @return {@code true} when the generation should report {@link HydrationState#STALE}
     */
    boolean isPastStaleness() {
        return !this.stalenessThreshold.isZero()
            && Duration.between(this.checkedAt, Instant.now()).compareTo(this.stalenessThreshold) >= 0;
    }

    /**
     * {@inheritDoc}
     *
     * <p>A generation the last rebuild published answers {@link HydrationState#STALE} once it has
     * gone unchecked past its staleness threshold, so a cadence that has stalled or stopped shows on
     * read without a tick having to run, while one whose ticks keep finding the origin unmoved stays
     * {@link HydrationState#CURRENT}.
     */
    @Override
    public @NotNull HydrationState getState() {
        HydrationState state = this.state;
        return state == HydrationState.CURRENT && this.isPastStaleness() ? HydrationState.STALE : state;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull ConcurrentList<T> getRows() throws JpaException {
        if (this.state == HydrationState.FAILED)
            throw new JpaException("Hydration failed for '%s' and there is nothing to serve", this.type.getName());

        return this.rows;
    }

    /**
     * Reads this type's rows from the given source without publishing them.
     *
     * <p>The rows come back unlinked and the held generation is untouched, so a reader keeps seeing
     * the previous one until {@link #hold(ConcurrentList)}.
     *
     * @param source where this type's rows are read from
     * @return the rows read, not yet linked
     * @throws JpaException if the source read fails, naming this type
     */
    @NotNull ConcurrentList<T> hydrate(@NotNull Source source) throws JpaException {
        this.state = this.hydratedAt.equals(Instant.EPOCH) ? HydrationState.HYDRATING : HydrationState.REFRESHING;

        try {
            ConcurrentList<T> read = source.read(this.type);
            read.forEach(entity -> {
                if (entity instanceof PostInit postInit)
                    postInit.postInit();
            });

            return read;
        } catch (Exception exception) {
            throw new JpaException(exception, "Failed to hydrate '%s'", this.type.getName());
        }
    }

    /**
     * Resolves every declared link on rows that have not been published.
     *
     * <p>A link reaches rows another type holds, so the session hands in the lookup: for a target
     * rebuilt in the same pass it answers that pass's rows, and otherwise the target's published
     * generation. Each target is asked for once per linking field.
     *
     * @param rows the unpublished rows to fill in
     * @param lookup answers a target type's rows, keyed by their stringified id
     * @throws JpaException if a single-valued link that is not an {@link Optional} carries no id or
     *         names no row
     */
    void link(
        @NotNull ConcurrentList<T> rows,
        @NotNull Function<Class<? extends JpaModel>, ConcurrentMap<String, ? extends JpaModel>> lookup
    ) {
        ConcurrentSet<FieldAccessor<?>> links = links(this.type);

        if (links.isEmpty())
            return;

        Reflection<?> reflection = new Reflection<>(this.type);
        ConcurrentMap<FieldAccessor<?>, ConcurrentMap<String, ? extends JpaModel>> lookups = Concurrent.newMap();
        links.forEach(field -> lookups.put(field, lookup.apply(targetOf(field))));

        rows.forEach(row -> this.resolveLinks(row, reflection, lookups));
    }

    /**
     * Publishes a generation by one reference write, then marks it current.
     *
     * @param replacement the linked rows to hold
     */
    void hold(@NotNull ConcurrentList<T> replacement) {
        Instant now = Instant.now();

        this.rows = replacement.toUnmodifiable();
        this.hydratedAt = now;
        this.checkedAt = now;
        this.state = HydrationState.CURRENT;
    }

    /**
     * Records that the source still holds the published generation, which restarts its cadence and
     * its staleness threshold without republishing it, so {@link #getHydratedAt()} still answers
     * when it was published.
     */
    void confirm() {
        this.checkedAt = Instant.now();
    }

    /**
     * Records that a rebuild failed, which is {@link HydrationState#FAILED} with nothing published
     * and {@link HydrationState#DEGRADED} while an earlier generation is still served.
     */
    void fail() {
        this.state = this.hydratedAt.equals(Instant.EPOCH) ? HydrationState.FAILED : HydrationState.DEGRADED;
    }

    /**
     * Resolves every link on one row against the lookups built for this generation.
     *
     * <p>The linking field's type decides what a miss does. A list keeps the rows its ids name and
     * drops the ids that name none. An {@link Optional} holds the row its id names, and is empty when
     * the id is absent or names no row. Any other field has no way to say that nothing was found, so
     * an absent id or one naming no row fails the link pass.
     *
     * @param entity the row to fill in
     * @param reflection the reflection over this repository's type
     * @param lookups the target rows, keyed, one entry per linking field
     * @throws JpaException if a single-valued link that is not an {@link Optional} carries no id or
     *         names no row
     */
    private void resolveLinks(
        @NotNull T entity,
        @NotNull Reflection<?> reflection,
        @NotNull ConcurrentMap<FieldAccessor<?>, ConcurrentMap<String, ? extends JpaModel>> lookups
    ) {
        for (Map.Entry<FieldAccessor<?>, ConcurrentMap<String, ? extends JpaModel>> link : lookups) {
            FieldAccessor<?> field = link.getKey();
            ConcurrentMap<String, ? extends JpaModel> lookup = link.getValue();
            Object held = unwrapped(reflection.getField(idPropertyOf(field)).get(entity));

            if (Collection.class.isAssignableFrom(field.getFieldType())) {
                Collection<?> ids = (Collection<?>) held;

                field.set(entity, ids == null ? Concurrent.newList() : ids.stream()
                    .map(id -> lookup.get(String.valueOf(id)))
                    .filter(Objects::nonNull)
                    .collect(Concurrent.toList()));

                continue;
            }

            JpaModel match = held == null ? null : lookup.get(String.valueOf(held));

            if (field.getFieldType() == Optional.class)
                field.set(entity, Optional.ofNullable(match));
            else if (held == null)
                throw new JpaException("Field '%s' of '%s' carries no id", field.getName(), this.type.getName());
            else if (match == null)
                throw new JpaException("Field '%s' of '%s' names '%s', which no row carries", field.getName(), this.type.getName(), held);
            else
                field.set(entity, match);
        }
    }

    /**
     * Reads the id a property carries, whether it holds one outright or wraps it in an
     * {@link Optional}.
     *
     * <p>The key is the id inside the wrapper, not the wrapper - {@code String.valueOf} on the wrapper
     * would produce {@code Optional[HUB]} and miss every row - and an empty wrapper carries no id.
     *
     * @param held the value the id property holds
     * @return the id, or {@code null} when the property carries none
     */
    private static @Nullable Object unwrapped(@Nullable Object held) {
        return held instanceof Optional<?> optional ? optional.orElse(null) : held;
    }

    /**
     * Finds every field on a type declaring a link.
     *
     * @param type the entity class to read
     * @return the linking fields, empty when the type declares none
     */
    static @NotNull ConcurrentSet<FieldAccessor<?>> links(@NotNull Class<?> type) {
        return new Reflection<>(type).getFields()
            .stream()
            .filter(field -> field.hasAnnotation(Linked.class))
            .collect(Concurrent.toSet());
    }

    /**
     * Reads the property a linking field resolves through.
     *
     * @param field the linking field
     * @return the name of the property carrying the id or ids
     */
    private static @NotNull String idPropertyOf(@NotNull FieldAccessor<?> field) {
        return field.getAnnotation(Linked.class).orElseThrow().value();
    }

    /**
     * Reads the model a linking or associating field resolves to: the type argument of an
     * {@link Optional} or of a list, and otherwise the field's own type.
     *
     * <p>Read through {@link FieldAccessor#getFieldType()} rather than {@code getType()}, which
     * answers the class that declares the field.
     *
     * <p>A collection must be able to hold the {@link ConcurrentList} a link resolves into, so a
     * {@code Set} or a list implementation is refused along with a map or an array, and the argument
     * must be a model class rather than a wildcard, a type variable or nothing at all.
     *
     * @param field the linking or associating field
     * @return the target entity class
     * @throws JpaException if the field names no model it can resolve to
     */
    static @NotNull Class<? extends JpaModel> targetOf(@NotNull FieldAccessor<?> field) {
        Class<?> declared = field.getFieldType();
        Type target = declared;

        if (declared == Optional.class) {
            target = field.getGenericType() instanceof ParameterizedType parameterized
                ? parameterized.getActualTypeArguments()[0]
                : null;
        } else if (Collection.class.isAssignableFrom(declared)) {
            target = declared.isAssignableFrom(ConcurrentList.class)
                && field.getGenericType() instanceof ParameterizedType parameterized
                ? parameterized.getActualTypeArguments()[0]
                : null;
        }

        if (target instanceof Class<?> type && JpaModel.class.isAssignableFrom(type))
            return type.asSubclass(JpaModel.class);

        throw new JpaException(
            "Field '%s' of '%s' names no model it can resolve to",
            field.getName(),
            field.getField().getDeclaringClass().getName()
        );
    }

}

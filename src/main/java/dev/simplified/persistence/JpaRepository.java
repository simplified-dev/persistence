package dev.simplified.persistence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.collection.query.Sortable;
import dev.simplified.collection.tuple.single.SingleStream;
import dev.simplified.gson.PostInit;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.util.time.Stopwatch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Default {@link Repository} implementation, holding one generation of rows read from a
 * {@link Source}.
 *
 * <p>A read answers from the held generation and performs no I/O, so every finder inherited from
 * {@link Sortable} is a scan or an index probe over rows already in
 * memory. Where those rows came from - a JSON document, a GitHub corpus, a database table - is the
 * source's business and changes nothing here.
 *
 * <p>A generation is built by {@link #hydrate()}, linked by {@link #link()} and published by one
 * reference write, so a reader holding one never sees it change underneath them.
 *
 * @param <T> the entity type, which must implement {@link JpaModel}
 * @see Repository
 * @see Source
 * @see JpaSession
 */
@Getter
public class JpaRepository<T extends JpaModel> implements Repository<T> {

    /**
     * The owning session, whose other repositories a link resolves against.
     */
    private final @NotNull JpaSession session;

    /**
     * The entity class managed by this repository.
     */
    private final @NotNull Class<T> type;

    /**
     * Where this type's rows come from.
     */
    private final @NotNull Source source;

    /**
     * {@code true} if the entity class declares any link to resolve.
     */
    @Getter(style = NamingStyle.FLUENT)
    private final boolean hasLinks;

    /**
     * How long to wait between rebuilds, or {@link Duration#ZERO} to hydrate once.
     */
    private final @NotNull Duration hydrationInterval;

    /**
     * How long a generation may stand before it reports {@link HydrationState#STALE}.
     */
    private final @NotNull Duration stalenessThreshold;

    /**
     * Whether a session waits for this type's first generation before handing back repositories.
     */
    @Getter(style = NamingStyle.FLUENT)
    private final boolean blocking;

    /**
     * The rows read by the most recent hydration, before links were resolved.
     */
    private volatile @NotNull ConcurrentList<T> rows = Concurrent.newUnmodifiableList();

    /**
     * The point this repository's generation has reached.
     */
    private volatile @NotNull HydrationState state = HydrationState.UNHYDRATED;

    /**
     * When the held generation was published.
     */
    private volatile @NotNull Instant hydratedAt = Instant.EPOCH;

    /**
     * Timing snapshot of the first hydration.
     */
    private @NotNull Stopwatch initialLoad = Stopwatch.of(Instant.now());

    /**
     * Timing snapshot of the most recent hydration.
     */
    private @NotNull Stopwatch lastRefresh = Stopwatch.of(Instant.now());

    /**
     * Creates a repository reading from the given source.
     *
     * <p>No I/O runs here. The generation is built when the session's hydrator reaches this type, so
     * a repository exists and answers {@link HydrationState#UNHYDRATED} before it holds anything.
     *
     * @param session the owning JPA session
     * @param type the entity class
     * @param source where this type's rows come from
     */
    JpaRepository(@NotNull JpaSession session, @NotNull Class<T> type, @NotNull Source source) {
        this.session = session;
        this.type = type;
        this.source = source;
        this.hasLinks = links(type).notEmpty();

        Hydration hydration = type.getAnnotation(Hydration.class);
        this.blocking = hydration == null || hydration.blocking();
        this.hydrationInterval = hydration == null
            ? Duration.ZERO
            : Duration.of(hydration.every(), hydration.unit().toChronoUnit());
        this.stalenessThreshold = hydration == null || hydration.stale() <= 0
            ? this.hydrationInterval.multipliedBy(2)
            : Duration.of(hydration.stale(), hydration.unit().toChronoUnit());
    }

    /**
     * Whether the held generation is past its freshness window.
     *
     * @return {@code true} when a rebuild is overdue
     */
    boolean isDue() {
        return !this.hydrationInterval.isZero()
            && Duration.between(this.hydratedAt, Instant.now()).compareTo(this.hydrationInterval) >= 0;
    }

    /**
     * Whether the held generation has stood past its staleness threshold.
     *
     * @return {@code true} when the generation should report {@link HydrationState#STALE}
     */
    boolean isPastStaleness() {
        return !this.stalenessThreshold.isZero()
            && Duration.between(this.hydratedAt, Instant.now()).compareTo(this.stalenessThreshold) >= 0;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull SingleStream<T> stream() throws JpaException {
        return SingleStream.of(this.getRows().stream());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull ConcurrentList<T> getRows() throws JpaException {
        if (this.state == HydrationState.FAILED)
            throw new JpaException("Hydration failed for '%s' and there is nothing to serve", this.type.getName());

        return this.rows;
    }

    /**
     * Reads this type's rows from its source and holds them, without resolving any link.
     *
     * <p>Links are resolved separately by {@link #link()} because a link reaches rows another
     * repository holds, and every repository has to have read before any of them can be linked.
     *
     * @throws JpaException if the source read fails
     */
    void hydrate() throws JpaException {
        Instant startedAt = Instant.now();
        boolean first = this.state == HydrationState.UNHYDRATED;
        this.state = first ? HydrationState.HYDRATING : HydrationState.REFRESHING;

        try {
            ConcurrentList<T> read = this.source.read(this.type);
            read.forEach(entity -> {
                if (entity instanceof PostInit postInit)
                    postInit.postInit();
            });

            this.rows = read.toUnmodifiable();
        } catch (Exception exception) {
            this.state = first ? HydrationState.FAILED : HydrationState.DEGRADED;
            throw exception instanceof JpaException jpaException
                ? jpaException
                : new JpaException(exception, "Failed to hydrate '%s'", this.type.getName());
        } finally {
            this.lastRefresh = Stopwatch.of(startedAt);

            if (first)
                this.initialLoad = this.lastRefresh;
        }
    }

    /**
     * Resolves every declared link on the held rows and publishes the generation.
     *
     * <p>Runs after every repository has hydrated, so a link into another type finds that type's rows
     * already read. Publication happens here rather than in {@link #hydrate()} because an index built
     * over rows whose links are still empty describes rows no reader will see.
     */
    void link() {
        if (this.state == HydrationState.FAILED || this.state == HydrationState.DEGRADED)
            return;

        if (this.hasLinks()) {
            Reflection<?> reflection = new Reflection<>(this.type);

            // One lookup per link, built once for the whole generation. Building it per row instead
            // would re-index the target's whole table for every row of this one.
            ConcurrentMap<FieldAccessor<?>, ConcurrentMap<String, ? extends JpaModel>> lookups = Concurrent.newMap();
            links(this.type).forEach(field -> lookups.put(field, this.lookupFor(targetOf(field))));

            this.rows.forEach(row -> this.resolveLinks(row, reflection, lookups));
        }

        this.hydratedAt = Instant.now();
        this.state = HydrationState.CURRENT;
    }

    /**
     * Marks the held generation as past its freshness window.
     */
    void markStale() {
        if (this.state == HydrationState.CURRENT)
            this.state = HydrationState.STALE;
    }

    /**
     * Indexes a target type's held rows by their key.
     *
     * @param target the type being linked to
     * @param <M> the target entity type
     * @return the target's rows keyed by their stringified id
     */
    private <M extends JpaModel> @NotNull ConcurrentMap<String, M> lookupFor(@NotNull Class<M> target) {
        Repository<M> repository = this.session.getRepository(target);
        return JpaModel.keyed(target, repository.getRows());
    }

    /**
     * Resolves every link on one row against the lookups built for this generation.
     *
     * @param entity the row to fill in
     * @param reflection the reflection over this repository's type
     * @param lookups the target rows, keyed, one entry per linking field
     */
    @SuppressWarnings("unchecked")
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
                Collection<String> ids = (Collection<String>) held;

                if (ids == null || ids.isEmpty()) {
                    field.set(entity, Concurrent.newList());
                    continue;
                }

                field.set(entity, ids.stream()
                    .map(lookup::get)
                    .filter(Objects::nonNull)
                    .collect(Concurrent.toList()));

                continue;
            }

            field.set(entity, held == null ? null : lookup.get(String.valueOf(held)));
        }
    }

    /**
     * Reads the id a property carries, whether it holds one outright or wraps it.
     *
     * <p>A link that may resolve to nothing declares its id as an {@link Optional}, and the id inside
     * it is the key, not the wrapper - {@code String.valueOf} on the wrapper would produce
     * {@code Optional[HUB]} and miss every row.
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
    private static @NotNull ConcurrentSet<FieldAccessor<?>> links(@NotNull Class<?> type) {
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
     * Reads the type a linking field resolves to, which is its element type when it holds many.
     *
     * <p>Read through {@link FieldAccessor#getFieldType()} rather than {@code getType()}, which
     * answers the class that declares the field.
     *
     * @param field the linking field
     * @return the target entity class
     */
    @SuppressWarnings("unchecked")
    private static @NotNull Class<? extends JpaModel> targetOf(@NotNull FieldAccessor<?> field) {
        if (!Collection.class.isAssignableFrom(field.getFieldType()))
            return (Class<? extends JpaModel>) field.getFieldType();

        ParameterizedType listType = (ParameterizedType) field.getGenericType();
        return (Class<? extends JpaModel>) listType.getActualTypeArguments()[0];
    }

    /**
     * Replaces the held generation outright, for a caller that already has the rows.
     *
     * @param replacement the rows to hold
     */
    void hold(@Nullable ConcurrentList<T> replacement) {
        this.rows = replacement == null ? Concurrent.newUnmodifiableList() : replacement.toUnmodifiable();
        this.hydratedAt = Instant.now();
        this.state = HydrationState.CURRENT;
    }

}

package dev.simplified.persistence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.collection.tuple.single.SingleStream;
import dev.simplified.gson.PostInit;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.store.Source;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.util.time.Stopwatch;
import jakarta.persistence.Id;
import org.hibernate.SessionFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * Default {@link Repository} implementation, holding one generation of rows read from a
 * {@link Source}.
 *
 * <p>A read answers from the held generation and performs no I/O, so every finder inherited from
 * {@link dev.simplified.collection.query.Sortable} is a scan or an index probe over rows already in
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
     * The owning session providing configuration and, for a relational origin, Hibernate access.
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
     * The {@link CacheExpiry} annotation from the entity class, or {@link CacheExpiry#DEFAULT}.
     */
    private final @NotNull CacheExpiry cacheExpiry;

    /**
     * The refresh interval derived from {@link #cacheExpiry}.
     */
    private final @NotNull Duration cacheDuration;

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
        this.cacheExpiry = Optional.ofNullable(type.getAnnotation(CacheExpiry.class)).orElse(CacheExpiry.DEFAULT);
        this.cacheDuration = Duration.of(this.cacheExpiry.value(), this.cacheExpiry.length().toChronoUnit());

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

        if (this.hasLinks())
            this.rows.forEach(this::resolveLinks);

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
     * Resolves every link on one row against the repositories holding their targets.
     *
     * @param entity the row to fill in
     */
    @SuppressWarnings("unchecked")
    private void resolveLinks(@NotNull T entity) {
        Reflection<?> reflection = new Reflection<>(this.type);

        for (FieldAccessor<?> field : links(this.type)) {
            String idProperty = idPropertyOf(field);
            Class<? extends JpaModel> target = targetOf(field);
            Repository<? extends JpaModel> repository = this.session.getRepository(target);
            Object held = reflection.getField(idProperty).get(entity);

            if (Collection.class.isAssignableFrom(field.getFieldType())) {
                Collection<String> ids = (Collection<String>) held;

                if (ids == null || ids.isEmpty()) {
                    field.set(entity, Concurrent.newList());
                    continue;
                }

                ConcurrentMap<String, ? extends JpaModel> lookup = keyed(repository);
                ConcurrentList<JpaModel> resolved = ids.stream()
                    .map(lookup::get)
                    .filter(Objects::nonNull)
                    .collect(Concurrent.toList());

                field.set(entity, resolved);
                continue;
            }

            if (held != null)
                field.set(entity, keyed(repository).get(String.valueOf(held)));
        }
    }

    /**
     * Indexes one repository's rows by their identifier.
     *
     * @param repository the repository whose rows are being reached into
     * @return the rows keyed by their stringified identifier
     */
    private static @NotNull ConcurrentMap<String, JpaModel> keyed(@NotNull Repository<? extends JpaModel> repository) {
        FieldAccessor<?> id = new Reflection<>(repository.getType()).getFields()
            .stream()
            .filter(field -> field.hasAnnotation(Id.class))
            .findFirst()
            .orElseThrow(() -> new JpaException("No @Id field found on entity: %s", repository.getType().getName()));

        ConcurrentMap<String, JpaModel> keyed = Concurrent.newMap();

        for (JpaModel row : repository.getRows()) {
            Object value = id.get(row);

            // Two rows stringifying to one id is a corpus mistake either way, and the first is the
            // one a reader scanning the table in order would have found.
            if (value != null)
                keyed.putIfAbsent(String.valueOf(value), row);
        }

        return keyed;
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
            .filter(field -> field.hasAnnotation(Linked.class) || field.hasAnnotation(ForeignIds.class))
            .collect(Concurrent.toSet());
    }

    /**
     * Reads the property a linking field resolves through.
     *
     * @param field the linking field
     * @return the name of the property carrying the id or ids
     */
    private static @NotNull String idPropertyOf(@NotNull FieldAccessor<?> field) {
        return field.getAnnotation(Linked.class)
            .map(Linked::value)
            .orElseGet(() -> field.getAnnotation(ForeignIds.class).orElseThrow().value());
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
     * Evicts the Hibernate L2 cache region for this entity type.
     *
     * @throws JpaException if cache eviction fails
     */
    public void evict() throws JpaException {
        try {
            SessionFactory sessionFactory = this.getSession().getSessionFactory();

            if (sessionFactory != null && sessionFactory.getCache() != null)
                sessionFactory.getCache().evict(this.getType());
        } catch (Exception ex) {
            throw new JpaException(ex);
        }
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

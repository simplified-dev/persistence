package dev.simplified.persistence;

import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.gson.PostInit;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.collection.tuple.single.LifecycleSingleStream;
import dev.simplified.collection.tuple.single.SingleStream;
import dev.simplified.util.time.Stopwatch;
import jakarta.persistence.Id;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.hibernate.NonUniqueObjectException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Default {@link Repository} implementation backed by an optional {@link Source}
 * with optional per-entity post-query processing via {@link #streamPeek}.
 *
 * <p>On construction the repository performs an immediate data load (via the source if
 * present, or a no-op for SQL-managed entities). Cache expiry is handled by JCache TTL
 * per entity type, derived from the {@link CacheExpiry} annotation; when entries expire,
 * Hibernate transparently re-queries the database on the next access.</p>
 *
 * <p>Each query executed through {@link #stream()} or {@link #stream(Session)} runs a
 * Hibernate criteria query against the L2-cached data, resolves any {@link ForeignIds}
 * transient fields, and applies the optional {@link #streamPeek} consumer.</p>
 *
 * @param <T> the entity type, which must implement {@link JpaModel}
 * @see Repository
 * @see Source
 * @see JpaSession
 */
@Getter
public class JpaRepository<T extends JpaModel> implements Repository<T> {

    /** The owning session providing Hibernate access and configuration. */
    private final @NotNull JpaSession session;

    /** The entity class managed by this repository. */
    private final @NotNull Class<T> type;

    /** The source used to load or reload data on each refresh cycle, or {@code empty} for SQL-managed entities. */
    private final @NotNull Optional<Source<T>> source;

    /** Optional consumer applied to each entity via {@link SingleStream#peek} on every query. */
    private final @NotNull Optional<Consumer<T>> streamPeek;

    /** {@code true} if the entity class has any {@link ForeignIds}-annotated fields. */
    @Accessors(fluent = true)
    private final boolean hasForeignIds;

    /** The {@link CacheExpiry} annotation from the entity class, or {@link CacheExpiry#DEFAULT}. */
    private final @NotNull CacheExpiry cacheExpiry;

    /** The refresh interval derived from {@link #cacheExpiry}. */
    private final @NotNull Duration cacheDuration;

    /** Accessor for the {@link Id}-annotated field, used by {@link #removeStaleEntities()}. */
    private final @NotNull Optional<FieldAccessor<?>> idAccessor;

    /** Timing snapshot of the initial data load performed during construction. */
    private final @NotNull Stopwatch initialLoad;

    /** Timing snapshot of the most recent refresh, updated on every {@link #refresh(boolean)} call. */
    private @NotNull Stopwatch lastRefresh;

    /** Entities loaded by the most recent {@link #persistToDatabase} call, consumed by {@link #removeStaleEntities()}. */
    @Getter(lombok.AccessLevel.NONE)
    private volatile @Nullable ConcurrentList<T> lastLoadedEntities;

    /**
     * Creates a repository with an optional source and stream peek.
     *
     * <p>Performs an immediate data load via the source (if present) and records the initial load timing.
     *
     * @param session the owning JPA session
     * @param type the entity class
     * @param source the source for loading data on refresh, or {@code null} for SQL-managed entities
     * @param streamPeek optional per-entity consumer applied on every {@link #stream()} call,
     *                   useful for re-attaching transient fields
     */
    JpaRepository(@NotNull JpaSession session, @NotNull Class<T> type, @Nullable Source<T> source, @Nullable Consumer<T> streamPeek) {
        this.session = session;
        this.type = type;
        this.source = Optional.ofNullable(source);
        this.streamPeek = Optional.ofNullable(streamPeek);

        // Cache @Id and @ForeignIds
        ConcurrentSet<FieldAccessor<?>> fields = new Reflection<>(type).getFields();
        this.idAccessor = fields.stream().filter(fa -> fa.hasAnnotation(Id.class)).findFirst();
        this.hasForeignIds = fields.stream().anyMatch(fa -> fa.hasAnnotation(ForeignIds.class));

        this.cacheExpiry = Optional.ofNullable(type.getAnnotation(CacheExpiry.class)).orElse(CacheExpiry.DEFAULT);
        this.cacheDuration = Duration.of(cacheExpiry.value(), cacheExpiry.length().toChronoUnit());
        this.refresh(true);
        this.initialLoad = this.lastRefresh;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull SingleStream<T> stream() throws JpaException {
        Session scopedSession = this.getSession().openScopedSession();
        try {
            return this.stream(scopedSession);
        } catch (RuntimeException ex) {
            try {
                scopedSession.close();
            } catch (Exception suppressed) {
                ex.addSuppressed(suppressed);
            }
            throw ex;
        }
    }

    /**
     * Executes a Hibernate criteria query as a lazy {@code getResultStream()} within the
     * given session, attaches an in-memory {@link ForeignIds} resolver and the optional
     * {@link #streamPeek} as {@code peek()} steps, and returns a {@link LifecycleSingleStream}
     * that owns the session and closes it on the first terminal operation.
     *
     * <p>The session passed in MUST be a fresh, caller-owned scoped session - typically opened
     * via {@link JpaSession#openScopedSession()}. The returned stream takes ownership of the
     * session lifecycle and closes it inside every terminal listed on
     * {@link LifecycleSingleStream}.</p>
     *
     * <p>For entities with {@link ForeignIds} fields, the FK target lookup maps are pre-built
     * via independent {@code targetRepo.stream()} sessions BEFORE the parent cursor is opened.
     * Per-element {@code session.find()} from inside a {@code peek} step would close the
     * parent's open {@code ResultSet} on single-cursor drivers like H2 (HHH-style "object is
     * already closed" exception), so the only safe path is to materialize the lookup tables
     * up front and use pure in-memory map access during peek.</p>
     *
     * @param session the caller-owned Hibernate session, transferred to the returned stream
     * @return a lifecycle-aware stream of all entities of type {@code T}
     * @throws JpaException if criteria query construction fails
     */
    public @NotNull SingleStream<T> stream(@NotNull Session session) throws JpaException {
        try {
            // Pre-build FK lookup maps via independent sessions BEFORE opening the parent
            // cursor. See method Javadoc for why per-element find() inside peek is unsafe.
            ConcurrentMap<String, ConcurrentMap<String, JpaModel>> fkLookups = this.hasForeignIds()
                ? this.buildForeignIdLookups()
                : Concurrent.newMap();

            CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
            CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(this.getType());
            Root<T> rootEntry = criteriaQuery.from(this.getType());
            criteriaQuery = criteriaQuery.select(rootEntry);

            Stream<T> jdkStream = session.createQuery(criteriaQuery).getResultStream();

            if (this.hasForeignIds())
                jdkStream = jdkStream.peek(entity -> this.resolveForeignIdsForOne(entity, fkLookups));

            if (this.streamPeek.isPresent())
                jdkStream = jdkStream.peek(this.streamPeek.get());

            return LifecycleSingleStream.of(jdkStream, session);
        } catch (Exception exception) {
            throw new JpaException(exception);
        }
    }

    /**
     * Executes only the source reload phase without cache eviction or warming
     * if {@code evictWarmCache} is {@code false}.
     * <p>
     * Forces an immediate full refresh cycle: source reload, cache eviction,
     * and cache warming if {@code evictWarmCache} is {@code true}.
     *
     * <p>Used by {@link JpaSession#refreshAll()} for coordinated multi-repository refresh
     * where eviction and warming are handled as separate phases.
     *
     * @throws JpaException if the source fails
     */
    void refresh(boolean evictWarmCache) throws JpaException {
        Instant startTime = Instant.now();

        try {
            this.source.ifPresent(this::persistToDatabase);

            if (evictWarmCache)
                this.evict();
        } catch (JpaException jpaEx) {
            throw jpaEx;
        } catch (Exception ex) {
            throw new JpaException(ex);
        } finally {
            this.lastRefresh = Stopwatch.of(startTime);
        }
    }

    /**
     * Upserts the given entities into the database via a {@link StatelessSession},
     * storing them for subsequent {@link #removeStaleEntities()} processing.
     *
     * <p>Uses {@link StatelessSession#upsertMultiple} to bypass dirty checking entirely,
     * avoiding HHH000502 warnings on {@code @JoinColumn(insertable = false, updatable = false)}
     * properties whose relationship references are null after JSON deserialization. Stale rows
     * are cleaned up separately by {@link #removeStaleEntities()} in reverse topological order.
     *
     * @param source the source to persist
     */
    void persistToDatabase(@NotNull Source<T> source) throws JpaException {
        ConcurrentList<T> entities = source.load(this);
        this.lastLoadedEntities = entities;

        entities.forEach(entity -> {
            if (entity instanceof PostInit)
                ((PostInit) entity).postInit();
        });

        try (StatelessSession statelessSession = this.getSession().getSessionFactory().openStatelessSession()) {
            statelessSession.getTransaction().begin();
            statelessSession.upsertMultiple(entities);
            statelessSession.getTransaction().commit();
        } catch (JpaException jpaEx) {
            throw jpaEx;
        } catch (Exception ex) {
            throw new JpaException(ex);
        }
    }

    /**
     * Deletes database rows whose IDs are not present in the most recent
     * {@link #persistToDatabase} call's entity list.
     *
     * <p>This method consumes and clears the stored entity list. It is a no-op if
     * {@link #persistToDatabase} has not been called since the last invocation,
     * if the loaded list was empty, or if no {@link Id} field exists on the entity.
     *
     * <p>Called by {@link JpaSession#refreshAll()} in reverse topological order
     * (children first) to ensure FK-safe stale removal.
     */
    void removeStaleEntities() {
        ConcurrentList<T> loaded = this.lastLoadedEntities;
        this.lastLoadedEntities = null;

        if (loaded == null || loaded.isEmpty() || this.idAccessor.isEmpty())
            return;

        FieldAccessor<?> idField = this.idAccessor.get();
        ConcurrentList<Object> validIds = loaded.stream()
            .map(entity -> idField.get(entity))
            .filter(Objects::nonNull)
            .collect(Concurrent.toList());

        if (validIds.isEmpty())
            return;

        this.getSession().transaction(hibernateSession -> {
            hibernateSession.createMutationQuery(
                "DELETE FROM " + this.getType().getSimpleName() + " WHERE " + idField.getName() + " NOT IN :ids"
            )
            .setParameter("ids", validIds)
            .executeUpdate();
        });
    }

    /**
     * Pre-builds the in-memory lookup maps for every {@link ForeignIds}-annotated field on
     * {@code T}. For each FK field this materializes the target repository (via an
     * independent scoped session) into a map keyed by the target's {@link Id} value.
     *
     * <p>Building these maps BEFORE the parent stream's cursor opens is the only safe
     * pattern on single-cursor JDBC drivers like H2 - issuing a {@code find()} on the
     * parent session inside a {@code peek} step would close the parent's open
     * {@code ResultSet} mid-iteration. The materialization is restricted to types whose
     * {@code @ForeignIds} sibling collection is non-empty on at least one parent entity,
     * but for simplicity we eagerly load every target type that any FK field references.</p>
     *
     * @return a map keyed by FK field name, whose value is a per-id lookup of target entities
     */
    @SuppressWarnings("unchecked")
    private @NotNull ConcurrentMap<String, ConcurrentMap<String, JpaModel>> buildForeignIdLookups() {
        ConcurrentMap<String, ConcurrentMap<String, JpaModel>> lookups = Concurrent.newMap();
        Reflection<?> reflection = new Reflection<>(this.getType());

        for (FieldAccessor<?> fieldAccessor : reflection.getFields()) {
            Optional<ForeignIds> annotation = fieldAccessor.getAnnotation(ForeignIds.class);
            if (annotation.isEmpty()) continue;

            ParameterizedType listType = (ParameterizedType) fieldAccessor.getGenericType();
            Class<? extends JpaModel> targetType = (Class<? extends JpaModel>) listType.getActualTypeArguments()[0];

            FieldAccessor<?> targetIdAccessor = new Reflection<>(targetType).getFields()
                .stream()
                .filter(fa -> fa.hasAnnotation(Id.class))
                .findFirst()
                .orElseThrow(() -> new JpaException("No @Id field found on entity: %s", targetType.getName()));

            Repository<? extends JpaModel> targetRepo = this.getSession().getRepository(targetType);
            ConcurrentList<? extends JpaModel> allTargets = targetRepo.findAll();

            ConcurrentMap<String, JpaModel> idMap = Concurrent.newMap();
            for (JpaModel target : allTargets) {
                Object targetId = targetIdAccessor.get(target);

                if (targetId != null)
                    idMap.put(String.valueOf(targetId), target);
            }

            lookups.put(fieldAccessor.getName(), idMap);
        }

        return lookups;
    }

    /**
     * Resolves {@link ForeignIds}-annotated transient fields on a single entity by looking
     * each id up in the pre-built {@code fkLookups} map, which was materialized before the
     * parent stream's cursor opened (see {@link #buildForeignIdLookups()}).
     *
     * <p>Invoked from a {@code peek()} step on the lazy result stream. Pure in-memory
     * lookup with no Hibernate involvement, so it cannot close the parent's
     * {@code ResultSet}.</p>
     *
     * @param entity the entity whose foreign id fields should be populated
     * @param fkLookups the pre-built lookup maps keyed by FK field name then by target id
     */
    @SuppressWarnings("unchecked")
    private void resolveForeignIdsForOne(@NotNull T entity, @NotNull ConcurrentMap<String, ConcurrentMap<String, JpaModel>> fkLookups) {
        Reflection<?> reflection = new Reflection<>(this.getType());

        for (FieldAccessor<?> fieldAccessor : reflection.getFields()) {
            Optional<ForeignIds> annotation = fieldAccessor.getAnnotation(ForeignIds.class);
            if (annotation.isEmpty()) continue;

            FieldAccessor<Collection<String>> idsAccessor = reflection.getField(annotation.get().value());

            Collection<String> ids = idsAccessor.get(entity);

            if (ids == null || ids.isEmpty()) {
                fieldAccessor.set(entity, Concurrent.newList());
                continue;
            }

            ConcurrentMap<String, JpaModel> idMap = fkLookups.get(fieldAccessor.getName());

            ConcurrentList<JpaModel> resolved = ids.stream()
                .map(idMap::get)
                .filter(Objects::nonNull)
                .collect(Concurrent.toList());

            fieldAccessor.set(entity, resolved);
        }
    }

    /**
     * Deletes the given entity within a new transaction.
     *
     * @param model the entity to delete
     * @return the deleted entity
     * @throws JpaException if the delete fails
     */
    public @NotNull T delete(@NotNull T model) throws JpaException {
        return this.getSession().transaction(session -> {
            return this.delete(session, model);
        });
    }

    /**
     * Deletes the given entity using the provided Hibernate session.
     *
     * @param session the Hibernate session to use
     * @param model the entity to delete
     * @return the deleted entity
     * @throws JpaException if the delete fails
     */
    public @NotNull T delete(@NotNull Session session, @NotNull T model) throws JpaException {
        try {
            session.remove(model);
            return model;
        } catch (Exception exception) {
            throw new JpaException(exception);
        }
    }

    /**
     * Evicts the Hibernate L2 cache region for this entity type.
     *
     * @throws JpaException if cache eviction fails
     */
    public void evict() throws JpaException {
        try {
            SessionFactory sessionFactory = this.getSession().getSessionFactory();

            if (sessionFactory.getCache() != null)
                sessionFactory.getCache().evict(this.getType());
        } catch (Exception ex) {
            throw new JpaException(ex);
        }
    }

    /**
     * Persists the given entity within a new transaction and returns it
     * with any generated keys populated.
     *
     * @param model the entity to persist
     * @return the persisted entity
     * @throws JpaException if the save fails
     */
    public @NotNull T save(@NotNull T model) throws JpaException {
        return this.getSession().transaction(session -> {
            return this.save(session, model);
        });
    }

    /**
     * Persists the given entity using the provided Hibernate session and
     * returns it with any generated keys populated.
     *
     * @param session the Hibernate session to use
     * @param model the entity to persist
     * @return the persisted entity
     * @throws JpaException if the save fails
     */
    public @NotNull T save(@NotNull Session session, @NotNull T model) throws JpaException {
        try {
            session.persist(model);
            return model;
        } catch (Exception exception) {
            throw new JpaException(exception);
        }
    }

    /**
     * Updates the given entity within a new transaction.
     *
     * @param model the entity to update
     * @return the updated entity
     * @throws JpaException if the update fails
     */
    public @NotNull T update(@NotNull T model) throws JpaException {
        return this.getSession().transaction(session -> {
            return this.update(session, model);
        });
    }

    /**
     * Updates the given entity using the provided Hibernate session.
     *
     * <p>Silently ignores {@link NonUniqueObjectException} if the entity
     * is already associated with the session.</p>
     *
     * @param session the Hibernate session to use
     * @param model the entity to update
     * @return the updated entity
     * @throws JpaException if the update fails for reasons other than a duplicate association
     */
    public @NotNull T update(@NotNull Session session, @NotNull T model) throws JpaException {
        try {
            session.merge(model);
            return model;
        } catch (NonUniqueObjectException nuoException) {
            return model;
        } catch (Exception exception) {
            throw new JpaException(exception);
        }
    }

}

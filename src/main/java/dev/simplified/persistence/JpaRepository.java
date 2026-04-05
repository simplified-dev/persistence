package dev.simplified.persistence;

import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.gson.PostInit;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.collection.tuple.single.SingleStream;
import dev.simplified.util.time.Stopwatch;
import jakarta.persistence.Id;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
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
@Log4j2
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
        return this.getSession().with((Function<Session, ? extends SingleStream<T>>) this::stream);
    }

    /**
     * Executes a Hibernate criteria query within the given session, resolves any
     * {@link ForeignIds} transient fields, applies the optional {@link #streamPeek},
     * and returns the results as a {@link SingleStream}.
     *
     * @param session the Hibernate session to query within
     * @return a stream of all entities of type {@code T}
     * @throws JpaException if the query fails
     */
    public @NotNull SingleStream<T> stream(@NotNull Session session) throws JpaException {
        try {
            CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
            CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(this.getType());
            Root<T> rootEntry = criteriaQuery.from(this.getType());
            criteriaQuery = criteriaQuery.select(rootEntry);

            List<T> results = session.createQuery(criteriaQuery)
                .setCacheRegion(this.getType().getName())
                .setCacheable(true)
                .getResultList();

            if (this.hasForeignIds())
                this.resolveForeignIds(results);

            SingleStream<T> result = SingleStream.of(results);

            if (this.streamPeek.isPresent())
                result = result.peek(this.streamPeek.get());

            return result;
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
            
            if (evictWarmCache) {
                this.evict();
                this.stream().close();
            }
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
     * Resolves {@link ForeignIds}-annotated transient fields on loaded entities by
     * matching ID strings against the target entity's {@link Id} field from its repository.
     *
     * @param entities the entities whose foreign ID fields should be populated
     */
    @SuppressWarnings("unchecked")
    private void resolveForeignIds(@NotNull List<T> entities) {
        if (entities.isEmpty()) return;

        Reflection<?> reflection = new Reflection<>(this.getType());

        for (FieldAccessor<?> fieldAccessor : reflection.getFields()) {
            Optional<ForeignIds> annotation = fieldAccessor.getAnnotation(ForeignIds.class);
            if (annotation.isEmpty()) continue;

            // Get the companion ID list field
            FieldAccessor<Collection<String>> idsAccessor = reflection.getField(annotation.get().value());

            // Determine target entity type from ConcurrentList<TargetType>
            ParameterizedType listType = (ParameterizedType) fieldAccessor.getGenericType();
            Class<? extends JpaModel> targetType = (Class<? extends JpaModel>) listType.getActualTypeArguments()[0];

            // Find the @Id field on the target entity
            FieldAccessor<?> targetIdAccessor = new Reflection<>(targetType).getFields()
                .stream()
                .filter(fa -> fa.hasAnnotation(Id.class))
                .findFirst()
                .orElseThrow(() -> new JpaException("No @Id field found on entity: %s", targetType.getName()));

            // Load all target entities once
            Repository<? extends JpaModel> targetRepo = this.getSession().getRepository(targetType);
            ConcurrentList<? extends JpaModel> allTargets = targetRepo.findAll();

            for (T entity : entities) {
                Collection<String> ids = idsAccessor.get(entity);

                if (ids == null || ids.isEmpty()) {
                    fieldAccessor.set(entity, Concurrent.newList());
                    continue;
                }

                ConcurrentList<JpaModel> resolved = allTargets.stream()
                    .filter(target -> {
                        Object targetId = targetIdAccessor.get(target);
                        return ids.contains(String.valueOf(targetId));
                    })
                    .collect(Concurrent.toList());

                fieldAccessor.set(entity, resolved);
            }
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

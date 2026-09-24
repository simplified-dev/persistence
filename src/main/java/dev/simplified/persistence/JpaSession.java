package dev.simplified.persistence;

import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.scheduler.Scheduler;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A JPA session holding one {@link JpaRepository} per registered type, every one of them read from the
 * single {@link Source} its {@link JpaConfig} names.
 *
 * <p>The session never asks what kind of source it holds. A database is opened by the caller, who
 * keeps it for Hibernate access and closes it once the session is shut down, so nothing here reaches
 * Hibernate.
 *
 * <p>Typical lifecycle managed by {@link SessionManager}:</p>
 * <ol>
 *     <li><b>Construction</b> - no I/O</li>
 *     <li>{@link #cacheRepositories()} - creates a {@link JpaRepository} per registered type and
 *         hydrates every one of them</li>
 *     <li>{@link #shutdown()} - clears repositories and shuts down the scheduler</li>
 * </ol>
 *
 * <p>Provides repository lookup via {@link #getRepository(Class)} and writes via
 * {@link #write(WriteRequest)}.</p>
 *
 * @see JpaConfig
 * @see JpaRepository
 * @see SessionManager
 */
public final class JpaSession {

    /**
     * Repositories keyed by their entity class.
     */
    private final @NotNull ConcurrentMap<Class<? extends JpaModel>, JpaRepository<? extends JpaModel>> repositories = Concurrent.newMap();

    /**
     * The registered models and the one source they are read from.
     */
    private final @NotNull JpaConfig config;

    /**
     * Internal scheduler for repository refresh tasks, shut down on {@link #shutdown()}.
     */
    private final @NotNull Scheduler scheduler;

    /**
     * {@code true} while this session has not been shut down.
     */
    @Getter private boolean active = true;

    /**
     * Constructs a session over the given configuration, performing no I/O.
     *
     * @param config the registered models and the source they are read from
     */
    JpaSession(@NotNull JpaConfig config) {
        this.config = config;
        this.scheduler = new Scheduler();
    }

    /**
     * Creates a {@link JpaRepository} for each registered model and performs the initial data load.
     *
     * <p>Called once, by {@link SessionManager#connect(JpaConfig)}.
     *
     * @throws JpaException if any registered type fails to hydrate
     */
    void cacheRepositories() {
        for (Class<JpaModel> model : this.config.models())
            this.repositories.put(model, new JpaRepository<>(this, model, this.config.source()));

        this.hydrate(this.config.models());

        // A type asks for a cadence through @Hydration; one that declares none is hydrated here
        // and left alone. The tick runs at the shortest declared interval and rebuilds only what
        // has come due.
        long minIntervalMs = this.repositories.values()
            .stream()
            .mapToLong(repository -> repository.getHydrationInterval().toMillis())
            .filter(ms -> ms > 0)
            .min()
            .orElse(0);

        if (minIntervalMs > 0)
            this.scheduler.scheduleAsync(this::hydrateDue, minIntervalMs, minIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Reads every given type, then resolves every link across them.
     *
     * <p>Two passes rather than one because a link reaches rows another repository holds, so every
     * repository has to have read before any of them can be linked.
     *
     * <p>A failing type aborts the pass. Continuing would publish a generation whose links point into
     * a type that never read, which is a wrong answer rather than a missing one.
     *
     * @param models the types to rebuild
     */
    private void hydrate(@NotNull Iterable<Class<JpaModel>> models) {
        ConcurrentList<JpaRepository<?>> hydrated = Concurrent.newList();

        for (Class<JpaModel> model : models) {
            JpaRepository<?> repository = this.repositories.get(model);
            repository.hydrate();
            hydrated.add(repository);
        }

        hydrated.forEach(JpaRepository::link);
    }

    /**
     * Rebuilds every type whose {@link Hydration} cadence has come due, and marks the rest stale when
     * they have stood too long.
     */
    private void hydrateDue() {
        ConcurrentList<Class<JpaModel>> due = Concurrent.newList();

        for (Class<JpaModel> model : this.config.models()) {
            JpaRepository<?> repository = this.repositories.get(model);

            if (repository.isDue())
                due.add(model);
            else if (repository.isPastStaleness())
                repository.markStale();
        }

        if (due.notEmpty())
            this.hydrate(due);
    }

    /**
     * Applies one write to the session's source, then rebuilds the written type.
     *
     * <p>The rebuild is what keeps a held generation honest after a write: the rows the origin now
     * holds are not the rows this session read. It is driven by the write rather than by a caller
     * asking for it, so nothing downstream gains a way to force a rehydration.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the session is inactive, the type is unregistered, or the source holds
     *         no write instruction
     */
    @SuppressWarnings("unchecked")
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        JpaRepository<M> repository = (JpaRepository<M>) this.getRepository(request.type());

        if (!(this.config.source() instanceof Source.Writable writable))
            throw new JpaException("Source for '%s' holds no write instruction", request.type().getName());

        writable.write(request);
        repository.hydrate();
        repository.link();
    }

    /**
     * Retrieves the {@link Repository} for the given model class, searching by exact key
     * match first, then by assignability.
     *
     * @param tClass the entity class to look up
     * @param <M> the entity type
     * @return the matching repository
     * @throws JpaException if the session is not active or no matching repository exists
     */
    @SuppressWarnings("unchecked")
    public <M extends JpaModel> @NotNull Repository<M> getRepository(@NotNull Class<M> tClass) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        if (this.repositories.containsKey(tClass))
            return (Repository<M>) this.repositories.get(tClass);

        for (Map.Entry<Class<? extends JpaModel>, JpaRepository<? extends JpaModel>> entry : this.repositories) {
            if (tClass.isAssignableFrom(entry.getKey()))
                return (Repository<M>) entry.getValue();
        }

        throw new JpaException("Repository for " + tClass.getName() + " not found");
    }

    /**
     * Checks whether a {@link Repository} for the given type (or a subtype) is registered
     * in this session.
     *
     * @param tClass the model class to check
     * @return {@code true} if a matching repository exists, {@code false} if not active or not found
     */
    public boolean hasRepository(@NotNull Class<?> tClass) {
        if (!this.isActive())
            return false;

        if (this.repositories.containsKey(tClass))
            return true;

        for (Map.Entry<Class<? extends JpaModel>, JpaRepository<? extends JpaModel>> entry : this.repositories) {
            if (tClass.isAssignableFrom(entry.getKey()))
                return true;
        }

        return false;
    }

    /**
     * Performs an orderly shutdown of this session.
     *
     * <p>Marks the session as inactive, clears all repositories and shuts down the internal
     * {@link Scheduler}. The source is not closed: whoever opened it closes it.</p>
     *
     * <p>After shutdown, {@link #getRepository(Class)} and {@link #hasRepository(Class)}
     * will reject or deny all lookups. The session object should be discarded.</p>
     */
    void shutdown() {
        this.active = false;
        this.repositories.clear();
        this.scheduler.shutdown();
    }

}

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
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A JPA session holding one {@link JpaRepository} per registered type, every one of them read from the
 * single {@link Source} its {@link JpaConfig} names.
 *
 * <p>The session is the hydrator: it is the only thing that reads a type's rows, links them and
 * publishes them. Each rebuild runs three passes over the types it covers - read every one, link every
 * one, then publish every one - so a link always reaches rows that have been read, and no reader sees
 * a row before its links resolve. Rebuilds run one at a time.
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
 *     <li>{@link #shutdown()} - clears repositories and shuts down the scheduler, if one was built</li>
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
     * Repositories keyed by their registered entity class.
     */
    private final @NotNull ConcurrentMap<Class<JpaModel>, JpaRepository<JpaModel>> repositories = Concurrent.newMap();

    /**
     * The registered models and the one source they are read from.
     */
    private final @NotNull JpaConfig config;

    /**
     * The scheduler driving {@link Hydration} cadences, built only when some registered type declares
     * one.
     */
    private @Nullable Scheduler scheduler;

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
            this.repositories.put(model, new JpaRepository<>(model));

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

        if (minIntervalMs > 0) {
            this.scheduler = new Scheduler();
            this.scheduler.scheduleAsync(this::hydrateDue, minIntervalMs, minIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Rebuilds the given types in three passes: read every one, link every one, publish every one.
     *
     * <p>A failing type aborts the rebuild before anything is published, and every type it covered
     * records the failure. Publishing the rest would hand out rows whose links point into a type that
     * never read, which is a wrong answer rather than a missing one.
     *
     * <p>Rebuilds run one at a time, so a write and a due tick never interleave their passes.
     *
     * @param types the registered types to rebuild, in registration order
     * @throws JpaException if any of them fails to read or link
     */
    private synchronized void hydrate(@NotNull ConcurrentList<Class<JpaModel>> types) {
        ConcurrentMap<Class<JpaModel>, ConcurrentList<JpaModel>> pass = Concurrent.newMap();
        ConcurrentMap<Class<? extends JpaModel>, ConcurrentMap<String, ? extends JpaModel>> keyed = Concurrent.newMap();

        try {
            for (Class<JpaModel> type : types)
                pass.put(type, this.repositories.get(type).hydrate(this.config.source()));

            for (Class<JpaModel> type : types)
                this.repositories.get(type).link(pass.get(type), target -> keyed.computeIfAbsent(target, key -> this.lookupFor(key, pass)));
        } catch (RuntimeException exception) {
            types.forEach(type -> this.repositories.get(type).fail());
            throw exception;
        }

        for (Class<JpaModel> type : types)
            this.repositories.get(type).hold(pass.get(type));
    }

    /**
     * Keys a link target's rows, taking them from the current pass when the target is being rebuilt
     * and from its published generation otherwise.
     *
     * @param target the type a link resolves to
     * @param pass the rows read by the current rebuild, keyed by type
     * @return the target's rows keyed by their stringified id
     * @throws JpaException if no registered type answers for the target
     */
    private @NotNull ConcurrentMap<String, JpaModel> lookupFor(
        @NotNull Class<? extends JpaModel> target,
        @NotNull ConcurrentMap<Class<JpaModel>, ConcurrentList<JpaModel>> pass
    ) {
        Class<JpaModel> type = registered(this.config.models(), target).orElseThrow(
            () -> new JpaException("A link reaches '%s', which this session does not register", target.getName())
        );

        ConcurrentList<JpaModel> rows = pass.containsKey(type) ? pass.get(type) : this.repositories.get(type).getRows();
        return JpaModel.keyed(type, rows);
    }

    /**
     * Rebuilds every type whose {@link Hydration} cadence has come due, and marks the rest stale when
     * they have stood too long.
     */
    private void hydrateDue() {
        ConcurrentList<Class<JpaModel>> due = Concurrent.newList();

        for (Class<JpaModel> model : this.config.models()) {
            JpaRepository<JpaModel> repository = this.repositories.get(model);

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
     * asking for it, and a request naming no rows writes and rebuilds nothing, so nothing downstream
     * gains a way to force a rehydration.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the session is inactive, the type is unregistered, or the source holds
     *         no write instruction
     */
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        Class<JpaModel> type = registered(this.config.models(), request.type()).orElseThrow(
            () -> new JpaException("Session holds no '%s' to write it", request.type().getName())
        );

        if (!(this.config.source() instanceof Source.Writable writable))
            throw new JpaException("Source for '%s' holds no write instruction", request.type().getName());

        if (request.rows().isEmpty())
            return;

        writable.write(request);
        this.hydrate(Concurrent.newList(type));
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

        return registered(this.config.models(), tClass)
            .map(type -> (Repository<M>) this.repositories.get(type))
            .orElseThrow(() -> new JpaException("Repository for " + tClass.getName() + " not found"));
    }

    /**
     * Checks whether a {@link Repository} for the given type (or a subtype) is registered
     * in this session.
     *
     * @param tClass the model class to check
     * @return {@code true} if a matching repository exists, {@code false} if not active or not found
     */
    public boolean hasRepository(@NotNull Class<?> tClass) {
        return this.isActive() && registered(this.config.models(), tClass).isPresent();
    }

    /**
     * Performs an orderly shutdown of this session.
     *
     * <p>Marks the session as inactive, clears all repositories and shuts down the scheduler, if one
     * was built. The source is not closed: whoever opened it closes it.</p>
     *
     * <p>After shutdown, {@link #getRepository(Class)} and {@link #hasRepository(Class)}
     * will reject or deny all lookups. The session object should be discarded.</p>
     */
    void shutdown() {
        this.active = false;
        this.repositories.clear();

        if (this.scheduler != null)
            this.scheduler.shutdown();
    }

    /**
     * Finds the registered type that answers for a class: the class itself when it is registered,
     * otherwise the first registered type it is assignable from.
     *
     * @param models the registered types, in registration order
     * @param type the class asked about
     * @return the registered type, empty when none answers
     */
    @SuppressWarnings("unchecked")
    private static @NotNull Optional<Class<JpaModel>> registered(
        @NotNull ConcurrentList<Class<JpaModel>> models,
        @NotNull Class<?> type
    ) {
        if (models.contains(type))
            return Optional.of((Class<JpaModel>) type);

        return models.stream()
            .filter(type::isAssignableFrom)
            .findFirst();
    }

}

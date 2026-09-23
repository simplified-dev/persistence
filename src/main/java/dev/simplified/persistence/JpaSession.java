package dev.simplified.persistence;

import com.google.gson.Gson;
import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.RelationalOrigin;
import dev.simplified.persistence.source.RelationalSource;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.scheduler.Scheduler;
import dev.simplified.util.time.Stopwatch;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A JPA session holding one {@link JpaRepository} per registered type.
 *
 * <p>The {@link JpaConfig#getDatabase() database} decides how much of this class exists. With one,
 * {@link RelationalSource} holds the whole Hibernate stack and is the origin for every type no factory
 * names a source for. Without one there is no database to register anything against, so none of it
 * runs and none of it needs to be on the classpath; every type reads through the {@link Source} its
 * factory declares.</p>
 *
 * <p>Typical lifecycle managed by {@link SessionManager}:</p>
 * <ol>
 *     <li><b>Construction</b> - with a database, opens it</li>
 *     <li>{@link #cacheRepositories()} - creates a {@link JpaRepository} per discovered model
 *         and hydrates every one of them</li>
 *     <li>{@link #shutdown()} - clears repositories, shuts down the scheduler and closes the
 *         database</li>
 * </ol>
 *
 * <p>Provides repository lookup via {@link #getRepository(Class)}, writes via
 * {@link #write(WriteRequest)}, and - for a session that opened a database - managed Hibernate access
 * via {@link #with(Consumer)} / {@link #with(Function)} and transactional execution via
 * {@link #transaction(Consumer)} / {@link #transaction(Function)}.</p>
 *
 * @see JpaConfig
 * @see JpaRepository
 * @see SessionManager
 */
@Getter
public final class JpaSession {

    /**
     * Cached repositories keyed by their entity class.
     */
    private final @NotNull ConcurrentMap<Class<? extends JpaModel>, Repository<? extends JpaModel>> repositories = Concurrent.newMap();

    /**
     * The entity classes discovered from the {@link RepositoryFactory}.
     */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /**
     * The immutable configuration that produced this session.
     */
    private final @NotNull JpaConfig config;

    /**
     * Internal scheduler for repository refresh tasks, shut down on {@link #shutdown()}.
     */
    private final @NotNull Scheduler scheduler;

    /**
     * Gson instance configured with this session's {@link GsonSettings}.
     */
    private final @NotNull Gson gson;

    /**
     * The open database, empty when this session opened none.
     */
    private final @NotNull Optional<RelationalSource> relational;

    /**
     * Timing snapshot of the full constructor bootstrap.
     */
    private final @NotNull Stopwatch initialization;

    /**
     * {@code true} while this session has not been shut down.
     */
    private boolean active = true;

    /**
     * Guard flag preventing {@link #cacheRepositories()} from running more than once.
     */
    private boolean repositoriesCached = false;

    /**
     * Timing snapshot of the {@link #cacheRepositories()} pass, or {@code null} if not yet run.
     */
    private Stopwatch repositoryCache;

    /**
     * Constructs a fully initialized session from the given configuration.
     *
     * <p>Resolves the model list via {@link RepositoryFactory#getModels()} and, when the config names
     * a {@link RelationalOrigin}, opens it. Without one there is nothing to open, and nothing on a
     * read path asks the question again.</p>
     *
     * @param config the configuration naming the database, the repository factory and the parser
     */
    public JpaSession(@NotNull JpaConfig config) {
        Instant startTime = Instant.now();
        this.models = config.getRepositoryFactory().getModels();
        this.config = config;
        this.scheduler = new Scheduler();
        this.gson = config.getGsonSettings().create();
        this.relational = config.getDatabase()
            .map(origin -> origin.open(this.models, this.gson, config.getLogLevel()));

        this.initialization = Stopwatch.of(startTime);
    }

    /**
     * The open database, for the paths that cannot run without one.
     *
     * @return the database this session opened
     * @throws JpaException if the session opened none
     */
    private @NotNull RelationalSource database() {
        return this.relational.orElseThrow(
            () -> new JpaException("Session opened no database, so there is no Hibernate access")
        );
    }

    /**
     * The Hibernate entity metadata, for a session that opened a database.
     *
     * @return the metadata
     * @throws JpaException if the session opened none
     */
    public @NotNull Metadata getMetadata() {
        return this.database().getMetadata();
    }

    /**
     * The Hibernate session factory, for a session that opened a database.
     *
     * @return the session factory
     * @throws JpaException if the session opened none
     */
    public @NotNull SessionFactory getSessionFactory() {
        return this.database().getSessionFactory();
    }


    /**
     * Creates a {@link JpaRepository} for each discovered model via the configured
     * {@link RepositoryFactory} and performs the initial data load.
     *
     * <p>This method may only be called once per session. Subsequent calls throw.</p>
     *
     * @throws JpaException if repositories have already been cached
     */
    public void cacheRepositories() {
        if (!this.isRepositoriesCached()) {
            this.repositoriesCached = true;
            Instant startTime = Instant.now();

            for (Class<JpaModel> model : this.models)
                this.repositories.put(model, this.createRepository(model));

            this.hydrate(this.models);
            this.repositoryCache = Stopwatch.of(startTime);

            // A type asks for a cadence through @Hydration; one that declares none is hydrated here
            // and left alone. The tick runs at the shortest declared interval and rebuilds only what
            // has come due.
            long minIntervalMs = this.repositories.values()
                .stream()
                .filter(JpaRepository.class::isInstance)
                .map(repository -> (JpaRepository<?>) repository)
                .mapToLong(repository -> repository.getHydrationInterval().toMillis())
                .filter(ms -> ms > 0)
                .min()
                .orElse(0);

            if (minIntervalMs > 0)
                this.scheduler.scheduleAsync(this::hydrateDue, minIntervalMs, minIntervalMs, TimeUnit.MILLISECONDS);
        } else
            throw new JpaException("Session has already cached repositories");
    }

    /**
     * Creates the repository for one entity type, reading its origin off the configured factory.
     *
     * <p>A factory naming no source for a type is saying the rows are the database's, so the open
     * database stands in. Substituting once, here, is what lets every repository hold a generation and
     * answer from an index without a read ever asking where its rows came from.
     *
     * @param type the entity class
     * @param <T> the entity type
     * @return the repository for that type
     * @throws JpaException if the type names no origin and the session opened no database
     */
    private <T extends JpaModel> @NotNull JpaRepository<T> createRepository(@NotNull Class<T> type) {
        Source declared = this.config.getRepositoryFactory()
            .sourceFor(type)
            .orElseGet(this::database);

        return new JpaRepository<>(this, type, declared);
    }

    /**
     * Reads every registered type, then resolves every link across them.
     *
     * <p>Two passes rather than one because a link reaches rows another repository holds, so every
     * repository has to have read before any of them can be linked. That is also what removes the
     * ordering constraint the model sort used to carry.
     *
     * <p>A failing type aborts the pass. Continuing would publish a generation whose links point into
     * a type that never read, which is a wrong answer rather than a missing one.
     *
     * @param models the types to rebuild
     */
    private void hydrate(@NotNull Iterable<Class<JpaModel>> models) {
        ConcurrentList<JpaRepository<?>> hydrated = Concurrent.newList();

        for (Class<JpaModel> model : models) {
            if (this.repositories.get(model) instanceof JpaRepository<?> repository) {
                repository.hydrate();
                hydrated.add(repository);
            }
        }

        hydrated.forEach(JpaRepository::link);
    }

    /**
     * Rebuilds every type whose {@link Hydration} cadence has come due, and marks the rest stale when
     * they have stood too long.
     */
    private void hydrateDue() {
        ConcurrentList<Class<JpaModel>> due = Concurrent.newList();

        for (Class<JpaModel> model : this.models) {
            if (!(this.repositories.get(model) instanceof JpaRepository<?> repository))
                continue;

            if (repository.isDue())
                due.add(model);
            else if (repository.isPastStaleness())
                repository.markStale();
        }

        if (due.notEmpty())
            this.hydrate(due);
    }

    /**
     * Applies one write to the origin that owns the type, then rebuilds that type.
     *
     * <p>The rebuild is what keeps a held generation honest after a write: the rows the origin now
     * holds are not the rows this session read. It is driven by the write rather than by a caller
     * asking for it, so nothing downstream gains a way to force a rehydration.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the session is inactive, the type is unregistered, or its source holds
     *         no write instruction
     */
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        if (!(this.getRepository(request.type()) instanceof JpaRepository<M> repository))
            throw new JpaException("Repository for '%s' cannot be written through", request.type().getName());

        if (!(repository.getSource() instanceof Source.Writable writable))
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

        for (Map.Entry<Class<? extends JpaModel>, Repository<? extends JpaModel>> entry : this.repositories) {
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

        for (Map.Entry<Class<? extends JpaModel>, Repository<? extends JpaModel>> entry : this.repositories) {
            if (tClass.isAssignableFrom(entry.getKey()))
                return true;
        }

        return false;
    }

    /**
     * Opens a new Hibernate {@link Session} from the open database.
     *
     * <p>The caller owns the returned session and must close it. This is the escape hatch a type
     * outside {@link RepositoryFactory#getModels()} is reached through - a registered type answers
     * from its held generation instead, and a write to one goes through
     * {@link #write(WriteRequest)}.
     *
     * @return a freshly opened session
     * @throws JpaException if the session opened no database
     */
    public @NotNull Session openSession() {
        return this.database().openSession();
    }

    /**
     * Opens a managed {@link Session}, passes it to the consumer, and auto-closes it.
     *
     * @param consumer the operation to perform with the session
     * @throws JpaException if the session cannot be opened or the operation fails
     */
    public void with(@NotNull Consumer<Session> consumer) {
        this.database().with(consumer);
    }

    /**
     * Opens a managed {@link Session}, passes it to the function, returns the result, and
     * auto-closes it.
     *
     * @param function the operation to perform with the session
     * @param <R> the return type
     * @return the result produced by the function
     * @throws JpaException if the session cannot be opened or the operation fails
     */
    public <R> R with(@NotNull Function<Session, R> function) {
        return this.database().with(function);
    }

    /**
     * Opens a managed {@link Session}, executes the consumer within a transaction, and auto-closes
     * it.
     *
     * @param consumer the transactional operation to perform
     * @throws JpaException if the session cannot be opened or the transaction fails
     */
    public void transaction(@NotNull Consumer<Session> consumer) {
        this.database().transaction(consumer);
    }

    /**
     * Opens a managed {@link Session}, executes the function within a transaction, returns the
     * result, and auto-closes it.
     *
     * @param function the transactional operation to perform
     * @param <R> the return type
     * @return the result produced by the function
     * @throws JpaException if the session cannot be opened or the transaction fails
     */
    public <R> R transaction(@NotNull Function<Session, R> function) {
        return this.database().transaction(function);
    }

    /**
     * Performs an orderly shutdown of this session.
     *
     * <p>Marks the session as inactive, clears all repositories, shuts down the internal
     * {@link Scheduler} and closes the database, if one was opened.</p>
     *
     * <p>After shutdown, {@link #getRepository(Class)} and {@link #hasRepository(Class)}
     * will reject or deny all lookups. The session object should be discarded.</p>
     */
    void shutdown() {
        this.active = false;
        this.repositories.clear();
        this.scheduler.shutdown();
        this.relational.ifPresent(RelationalSource::close);
    }

}

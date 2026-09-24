package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

/**
 * Thread-safe registry of active {@link JpaSession} instances, providing the primary
 * entry points for session lifecycle management and cross-session repository lookup.
 *
 * <p>Sessions are created via {@link #connect(JpaConfig)}, which constructs a {@link JpaSession},
 * hydrates every type it registers, and only then adds it to the internal list - so no lookup ever
 * reaches a session that has not finished its first hydration, or one whose first hydration
 * failed.</p>
 *
 * <p>Repository access via {@link #getRepository(Class)} searches all active sessions
 * in registration order, returning the first match. This allows multiple sessions
 * (e.g. separate H2 instances for different model sets) to coexist transparently.</p>
 *
 * <p>Shutting a manager down is optional: a JVM shutdown hook shuts every session it still holds
 * down at exit. The hook is registered only while the manager holds a session - {@link #connect}
 * registers it with the first, and a shutdown that leaves none removes it - so a manager shut down
 * to empty is no longer reachable through the JVM, and can be connected again. JVM shutdown hooks
 * run concurrently, so at exit a database's own hook can close it while a rebuild or tick of a
 * session reading it is still running, and that rebuild or tick fails.</p>
 *
 * @see JpaSession
 * @see JpaConfig
 */
public final class SessionManager {

    /**
     * The sessions this manager holds, in registration order. Its monitor guards the hook
     * bookkeeping.
     */
    private final @NotNull ConcurrentList<JpaSession> sessions = Concurrent.newList();

    /**
     * JVM shutdown hook that shuts every held session down at exit, registered while this manager
     * holds a session.
     */
    private final @NotNull Thread shutdownHook = new Thread(this::shutdown, "session-manager-shutdown");

    /**
     * Creates a new {@link JpaSession} from the given configuration, hydrates it, and registers it.
     *
     * <p>The returned session has hydrated every registered type and is immediately usable for
     * queries. The first session a manager holds registers its JVM shutdown hook. When the first
     * hydration fails the session is shut down and never registered; a database the caller opened
     * for it stays open until the caller closes it or the JVM exits.</p>
     *
     * @param config the registered models and the source they are read from
     * @return the newly created and fully initialized session
     * @throws JpaException if a registered type declares a collection-valued, element-collection or
     *         lazy association, or a link or association naming no model it can resolve to, which is
     *         refused before anything is read; or if a registered type fails to read or link
     * @throws IllegalStateException if this manager holds no session and the JVM is already exiting
     */
    public @NotNull JpaSession connect(@NotNull JpaConfig config) {
        JpaSession session = new JpaSession(config);

        try {
            session.cacheRepositories();

            synchronized (this.sessions) {
                if (this.sessions.isEmpty())
                    Runtime.getRuntime().addShutdownHook(this.shutdownHook);

                this.sessions.add(session);
            }
        } catch (RuntimeException exception) {
            session.shutdown();
            throw exception;
        }

        return session;
    }

    /**
     * Shuts down and removes all managed sessions.
     *
     * <p>Each active session is {@linkplain JpaSession#shutdown() shut down} before the
     * internal list is cleared, and the JVM shutdown hook is removed with the last of them. After
     * this call, {@link #isActive()} returns {@code false}; the manager can be connected again.
     * Shutting a session down never closes the database it reads.</p>
     */
    public void shutdown() {
        this.sessions.forEach(this::shutdown);
    }

    /**
     * Shuts down and removes a single session from this manager.
     *
     * <p>If the session is still active, {@link JpaSession#shutdown()} is called before
     * removal. Removing the last session this manager holds removes its JVM shutdown hook, unless
     * the JVM is already exiting, in which case the hook is left to the JVM. The session object
     * should be discarded after this call.</p>
     *
     * @param session the session to disconnect and remove
     */
    public void shutdown(@NotNull JpaSession session) {
        if (session.isActive())
            session.shutdown();

        synchronized (this.sessions) {
            if (this.sessions.remove(session) && this.sessions.isEmpty()) {
                try {
                    Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
                } catch (IllegalStateException ignore) { }
            }
        }
    }

    /**
     * Searches all active sessions for a {@link Repository} matching the given model class
     * and returns the first match.
     *
     * @param tClass the entity class to look up
     * @param <M> the entity type
     * @return the first matching repository found across all sessions
     * @throws JpaException if no active sessions exist or no session contains a matching repository
     */
    public <M extends JpaModel> @NotNull Repository<M> getRepository(@NotNull Class<M> tClass) {
        if (!this.isActive())
            throw new JpaException("There are no active sessions");

        for (JpaSession session : this.sessions) {
            Optional<Repository<M>> repository = session.getRepository(tClass);

            if (repository.isPresent())
                return repository.get();
        }

        throw new JpaException("Repository cannot be retrieved");
    }

    /**
     * Applies one write through the session that registers the type, and rebuilds that type and
     * every type linking into it.
     *
     * <p>The symmetric member to {@link #getRepository(Class)}: a consumer that reaches a
     * repository through this registry writes through it too, rather than having to hold on to
     * whichever session it connected. A write goes to the session registering the request's exact
     * type, and succeeds only where that session reads a {@link Source.Writable}.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if no active session registers the type, its source holds no write
     *         instruction, an upserted row's link that is neither a list nor an {@link Optional}
     *         carries no id or names no row, or the write fails
     */
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("There are no active sessions");

        for (JpaSession session : this.sessions) {
            if (session.getRepository(request.type()).filter(repository -> repository.getType() == request.type()).isPresent()) {
                session.write(request);
                return;
            }
        }

        throw new JpaException("No session holds '%s' to write it", request.type().getName());
    }

    /**
     * Checks whether at least one managed session is currently active.
     *
     * @return {@code true} if any session reports {@link JpaSession#isActive()}
     */
    public boolean isActive() {
        return this.sessions.stream().anyMatch(JpaSession::isActive);
    }

}

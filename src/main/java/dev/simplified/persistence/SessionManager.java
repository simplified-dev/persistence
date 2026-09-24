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
 * @see JpaSession
 * @see JpaConfig
 */
public final class SessionManager {

    private final @NotNull ConcurrentList<JpaSession> sessions = Concurrent.newList();

    public SessionManager() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "session-manager-shutdown"));
    }

    /**
     * Creates a new {@link JpaSession} from the given configuration, hydrates it, and registers it.
     *
     * <p>The returned session has hydrated every registered type and is immediately usable for
     * queries. When the first hydration fails the session is shut down and never registered; a
     * database the caller opened for it stays open until the caller closes it.</p>
     *
     * @param config the registered models and the source they are read from
     * @return the newly created and fully initialized session
     * @throws JpaException if a registered type fails to hydrate
     */
    public @NotNull JpaSession connect(@NotNull JpaConfig config) {
        JpaSession session = new JpaSession(config);

        try {
            session.cacheRepositories();
        } catch (RuntimeException exception) {
            session.shutdown();
            throw exception;
        }

        this.sessions.add(session);
        return session;
    }

    /**
     * Shuts down and removes all managed sessions.
     *
     * <p>Each active session is {@linkplain JpaSession#shutdown() shut down} before the
     * internal list is cleared. After this call, {@link #isActive()} returns {@code false}.</p>
     */
    public void shutdown() {
        this.sessions.forEach(this::shutdown);
    }

    /**
     * Shuts down and removes a single session from this manager.
     *
     * <p>If the session is still active, {@link JpaSession#shutdown()} is called before
     * removal. The session object should be discarded after this call.</p>
     *
     * @param session the session to disconnect and remove
     */
    public void shutdown(@NotNull JpaSession session) {
        if (session.isActive())
            session.shutdown();

        this.sessions.remove(session);
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
     *         instruction, or the rebuild after the applied write fails
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

package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;

/**
 * Thread-safe registry of active {@link JpaSession} instances, providing the primary
 * entry points for session lifecycle management and cross-session repository lookup.
 *
 * <p>Sessions are created via {@link #connect(JpaConfig)}, which constructs and initializes
 * a {@link JpaSession}, caches its repositories, and adds it to the internal list.</p>
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
     * Creates a new {@link JpaSession} from the given configuration, registers it,
     * and populates its repository cache.
     *
     * <p>The returned session has hydrated every registered type and is immediately usable for
     * queries.</p>
     *
     * @param config the registered models and the source they are read from
     * @return the newly created and fully initialized session
     * @throws JpaException if a registered type fails to hydrate
     */
    public @NotNull JpaSession connect(@NotNull JpaConfig config) {
        JpaSession session = new JpaSession(config);
        this.sessions.add(session);
        session.cacheRepositories();
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
            if (session.hasRepository(tClass))
                return session.getRepository(tClass);
        }

        throw new JpaException("Repository cannot be retrieved");
    }

    /**
     * Applies one write through the session that holds the type, and rebuilds that type.
     *
     * <p>The symmetric member to {@link #getRepository(Class)}: a consumer that reaches a
     * repository through this registry writes through it too, rather than having to hold on to
     * whichever session it connected.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if no active session holds the type, or its source holds no write
     *         instruction
     */
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("There are no active sessions");

        for (JpaSession session : this.sessions) {
            if (session.hasRepository(request.type())) {
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

package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * Thread-safe registry of active {@link JpaSession} instances, providing the primary
 * entry points for session lifecycle management and cross-session repository lookup.
 *
 * <p>Sessions are created via {@link #connect(JpaConfig)}, which constructs and initializes
 * a {@link JpaSession}, caches its repositories, and adds it to the internal list.
 * Duplicate connections (same {@link JpaConfig#getUniqueId()}) are rejected.</p>
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
     * <p>The session is fully initialized (Hibernate bootstrap complete) before
     * {@link JpaSession#cacheRepositories()} is called. The returned session is
     * immediately usable for queries.</p>
     *
     * @param config the configuration defining driver, repository factory, and connection settings
     * @return the newly created and fully initialized session
     * @throws JpaException if a session with the same {@link JpaConfig#getUniqueId()} is already registered
     */
    public @NotNull JpaSession connect(@NotNull JpaConfig config) {
        if (this.isRegistered(config))
            throw new JpaException("Session with the specified identifier is already active");

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
     * Shuts down and removes the session matching the given configuration's unique ID.
     *
     * <p>If no session matches, this method does nothing.</p>
     *
     * @param config the configuration identifying the session to disconnect
     */
    public void shutdown(@NotNull JpaConfig config) {
        this.sessions.stream()
            .filter(session -> session.getConfig().getUniqueId().equals(config.getUniqueId()))
            .findFirst()
            .ifPresent(this::shutdown);
    }

    /**
     * Checks whether a session with the same {@link JpaConfig#getUniqueId()} is already
     * managed by this registry.
     *
     * @param config the configuration to check
     * @return {@code true} if a session with a matching unique ID exists
     */
    public boolean isRegistered(@NotNull JpaConfig config) {
        return this.sessions.stream().anyMatch(session -> session.getConfig().getUniqueId().equals(config.getUniqueId()));
    }

    /**
     * Tears down all current sessions and reconnects them from their stored configurations.
     *
     * <p>Captures each session's {@link JpaConfig}, calls {@link #shutdown()}, then
     * re-invokes {@link #connect(JpaConfig)} for each config. Useful for resetting
     * the in-memory state without rebuilding configuration objects.</p>
     */
    public void reconnect() {
        ConcurrentList<JpaConfig> configs = this.sessions.stream()
            .map(JpaSession::getConfig)
            .collect(Concurrent.toList());

        this.shutdown();
        configs.forEach(this::connect);
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
     * Returns an unmodifiable snapshot of all currently managed sessions.
     *
     * @return an unmodifiable copy of the session list
     */
    public @NotNull ConcurrentList<JpaSession> getSessions() {
        return this.sessions.toUnmodifiable();
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

package dev.simplified.persistence.driver;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for database driver configuration, providing JDBC connection URLs,
 * Hibernate dialect mapping, and driver availability checks.
 */
public interface JpaDriver {

    /**
     * Builds the JDBC connection URL using the {@linkplain #getDefaultPort() default port}.
     *
     * @param host the database server hostname or IP address
     * @param schema the target database/schema name
     * @return the fully-formed JDBC connection URL
     */
    default @NotNull String getConnectionUrl(@NotNull String host, @NotNull String schema) {
        return this.getConnectionUrl(host, this.getDefaultPort(), schema);
    }

    /**
     * Builds the JDBC connection URL for the given host, port, and schema.
     *
     * @param host the database server hostname or IP address
     * @param port the port number to connect on
     * @param schema the target database/schema name
     * @return the fully-formed JDBC connection URL
     */
    @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema);

    /** The default port number for this database type. */
    int getDefaultPort();

    /** The fully-qualified name of the Hibernate dialect class for this database type. */
    @NotNull String getDialectClass();

    /** The fully-qualified class path of the JDBC driver implementation for this database. */
    @NotNull String getClassPath();

    /**
     * Attempts to load the driver class specified by the {@code getClassPath()}.
     * <p>
     * If the class cannot be found, it indicates that the driver is unavailable.
     *
     * @return {@code true} if the driver class can be successfully loaded and is available, {@code false} otherwise.
     */
    default boolean isAvailable() {
        try {
            Class.forName(this.getClassPath());
            return true;
        } catch (ClassNotFoundException cnfException) {
            return false;
        }
    }

    /**
     * Returns {@code true} if this driver targets an embedded or in-memory database
     * that does not require external connection details (host, port, credentials)
     * or a connection pool.
     */
    default boolean isEmbedded() {
        return false;
    }

}

package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * A PostgreSQL database, reached over the network and maintained elsewhere.
 */
@Getter
public final class PostgreSqlDriver implements JpaDriver {

    /**
     * The port PostgreSQL listens on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 5432;

    private final @NotNull String dialectClass = "org.hibernate.dialect.PostgreSQLDialect";
    private final @NotNull String classPath = "org.postgresql.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    private PostgreSqlDriver() {}

    /**
     * Names a database on the default port.
     *
     * @param host the server hostname or address
     * @param schema the database name
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalSource.Authenticating at(@NotNull String host, @NotNull String schema) {
        return at(host, DEFAULT_PORT, schema);
    }

    /**
     * Names a database.
     *
     * @param host the server hostname or address
     * @param port the port the server listens on
     * @param schema the database name
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalSource.Authenticating at(@NotNull String host, int port, @NotNull String schema) {
        return RelationalSource.authenticating(
            new PostgreSqlDriver(),
            String.format("jdbc:postgresql://%s:%s/%s", host, port, schema)
        );
    }

}

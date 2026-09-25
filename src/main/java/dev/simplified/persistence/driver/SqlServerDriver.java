package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.Connection;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * A Microsoft SQL Server database, reached over the network and maintained elsewhere.
 */
@Getter
public final class SqlServerDriver implements JpaDriver {

    /**
     * The port SQL Server listens on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 1433;

    private final @NotNull String dialectClass = "org.hibernate.dialect.SQLServerDialect";
    private final @NotNull String classPath = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    private SqlServerDriver() {}

    /**
     * Names a database on the default port.
     *
     * @param host the server hostname or address
     * @param schema the database name
     * @param credentials the account it is reached with
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull String host, @NotNull String schema, @NotNull Connection.Credentials credentials) {
        return at(host, DEFAULT_PORT, schema, credentials);
    }

    /**
     * Names a database.
     *
     * @param host the server hostname or address
     * @param port the port the server listens on
     * @param schema the database name
     * @param credentials the account it is reached with
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull String host, int port, @NotNull String schema, @NotNull Connection.Credentials credentials) {
        return RelationalSource.builder()
            .withDriver(new SqlServerDriver())
            .withUrl(String.format("jdbc:sqlserver://%s:%s;databaseName=%s", host, port, schema))
            .withCredentials(credentials)
            .build();
    }

}

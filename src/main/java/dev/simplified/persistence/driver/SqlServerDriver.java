package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
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
    private final @NotNull String classPath = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    private SqlServerDriver() {}

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
            new SqlServerDriver(),
            String.format("jdbc:microsoft:sqlserver://%s:%s;DatabaseName=%s", host, port, schema)
        );
    }

}

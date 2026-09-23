package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.RelationalOrigin;
import org.jetbrains.annotations.NotNull;

/**
 * A MariaDB database, reached over the network and maintained elsewhere.
 */
@Getter
public final class MariaDbDriver implements JpaDriver {

    /**
     * The port MariaDB listens on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 3306;

    private final @NotNull String dialectClass = "org.hibernate.dialect.MariaDBDialect";
    private final @NotNull String classPath = "org.mariadb.jdbc.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    private MariaDbDriver() {}

    /**
     * Names a schema on the default port.
     *
     * @param host the server hostname or address
     * @param schema the schema name
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalOrigin.Authenticating at(@NotNull String host, @NotNull String schema) {
        return at(host, DEFAULT_PORT, schema);
    }

    /**
     * Names a schema.
     *
     * @param host the server hostname or address
     * @param port the port the server listens on
     * @param schema the schema name
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalOrigin.Authenticating at(@NotNull String host, int port, @NotNull String schema) {
        return RelationalOrigin.authenticating(
            new MariaDbDriver(),
            String.format("jdbc:mariadb://%s:%s/%s", host, port, schema)
        );
    }

}

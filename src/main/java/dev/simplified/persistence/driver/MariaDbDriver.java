package dev.simplified.persistence.driver;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.persistence.source.Connection;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * A MariaDB database, reached over the network and maintained elsewhere.
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MariaDbDriver implements JpaDriver {

    /**
     * The port MariaDB listens on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 3306;

    private final @NotNull String dialectClass = "org.hibernate.dialect.MariaDBDialect";
    private final @NotNull String classPath = "org.mariadb.jdbc.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    /**
     * Names a schema on the default port.
     *
     * @param host the server hostname or address
     * @param schema the schema name
     * @param credentials the account it is reached with
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull String host, @NotNull String schema, @NotNull Connection.Credentials credentials) {
        return at(host, DEFAULT_PORT, schema, credentials);
    }

    /**
     * Names a schema.
     *
     * @param host the server hostname or address
     * @param port the port the server listens on
     * @param schema the schema name
     * @param credentials the account it is reached with
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull String host, int port, @NotNull String schema, @NotNull Connection.Credentials credentials) {
        return RelationalSource.builder()
            .withDriver(new MariaDbDriver())
            .withUrl(String.format("jdbc:mariadb://%s:%s/%s", host, port, schema))
            .withCredentials(credentials)
            .build();
    }

}

package dev.simplified.persistence.driver;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.persistence.source.Connection;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * An Oracle database reached through the Thin JDBC driver, maintained elsewhere.
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OracleThinDriver implements JpaDriver {

    /**
     * The port an Oracle listener answers on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 1521;

    private final @NotNull String dialectClass = "org.hibernate.dialect.OracleDialect";
    private final @NotNull String classPath = "oracle.jdbc.driver.OracleDriver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    /**
     * Names a service on the default port.
     *
     * @param host the server hostname or address
     * @param schema the service identifier
     * @param credentials the account it is reached with
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull String host, @NotNull String schema, @NotNull Connection.Credentials credentials) {
        return at(host, DEFAULT_PORT, schema, credentials);
    }

    /**
     * Names a service.
     *
     * @param host the server hostname or address
     * @param port the port the listener answers on
     * @param schema the service identifier
     * @param credentials the account it is reached with
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull String host, int port, @NotNull String schema, @NotNull Connection.Credentials credentials) {
        return RelationalSource.builder()
            .withDriver(new OracleThinDriver())
            .withUrl(String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, schema))
            .withCredentials(credentials)
            .build();
    }

}

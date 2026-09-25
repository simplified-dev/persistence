package dev.simplified.persistence.driver;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NoArgsConstructor;
import dev.simplified.persistence.source.Connection;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * An H2 database reached over TCP, served by an H2 server holding it in a file or in memory.
 *
 * <p>The server maintains the schema, so nothing is created or dropped from here.
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class H2TcpDriver implements JpaDriver {

    /**
     * The port an H2 server listens on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 9092;

    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

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
            .withDriver(new H2TcpDriver())
            .withUrl(String.format("jdbc:h2:tcp://%s:%s/%s", host, port, schema))
            .withCredentials(credentials)
            .build();
    }

}

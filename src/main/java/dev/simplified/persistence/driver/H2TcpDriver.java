package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.RelationalOrigin;
import org.jetbrains.annotations.NotNull;

/**
 * An H2 database reached over TCP, served by an H2 server holding it in a file or in memory.
 *
 * <p>The server maintains the schema, so nothing is created or dropped from here.
 */
@Getter
public final class H2TcpDriver implements JpaDriver {

    /**
     * The port an H2 server listens on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 9092;

    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    private H2TcpDriver() {}

    /**
     * Names a database on the default port.
     *
     * @param host the server hostname or address
     * @param schema the database name
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalOrigin.Authenticating at(@NotNull String host, @NotNull String schema) {
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
    public static @NotNull RelationalOrigin.Authenticating at(@NotNull String host, int port, @NotNull String schema) {
        return RelationalOrigin.authenticating(
            new H2TcpDriver(),
            String.format("jdbc:h2:tcp://%s:%s/%s", host, port, schema)
        );
    }

}

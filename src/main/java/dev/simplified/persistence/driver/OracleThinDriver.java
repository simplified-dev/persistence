package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.RelationalOrigin;
import org.jetbrains.annotations.NotNull;

/**
 * An Oracle database reached through the Thin JDBC driver, maintained elsewhere.
 */
@Getter
public final class OracleThinDriver implements JpaDriver {

    /**
     * The port an Oracle listener answers on unless told otherwise.
     */
    private static final int DEFAULT_PORT = 1571;

    private final @NotNull String dialectClass = "org.hibernate.dialect.OracleDialect";
    private final @NotNull String classPath = "oracle.jdbc.driver.OracleDriver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.EXTERNAL;

    private OracleThinDriver() {}

    /**
     * Names a service on the default port.
     *
     * @param host the server hostname or address
     * @param schema the service identifier
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalOrigin.Authenticating at(@NotNull String host, @NotNull String schema) {
        return at(host, DEFAULT_PORT, schema);
    }

    /**
     * Names a service.
     *
     * @param host the server hostname or address
     * @param port the port the listener answers on
     * @param schema the service identifier
     * @return the step that takes the credentials
     */
    public static @NotNull RelationalOrigin.Authenticating at(@NotNull String host, int port, @NotNull String schema) {
        return RelationalOrigin.authenticating(
            new OracleThinDriver(),
            String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, schema)
        );
    }

}

package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for Oracle databases using the Thin JDBC driver
 * (default port 1571).
 */
@Getter
public class OracleThinDriver implements JpaDriver {

    private final int defaultPort = 1571;
    private final @NotNull String dialectClass = "org.hibernate.dialect.OracleDialect";
    private final @NotNull String classPath = "oracle.jdbc.driver.OracleDriver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, schema);
    }

}

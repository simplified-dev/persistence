package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for Microsoft SQL Server databases (default port 1433).
 */
@Getter
public class SqlServerDriver implements JpaDriver {

    private final int defaultPort = 1433;
    private final @NotNull String dialectClass = "org.hibernate.dialect.SQLServerDialect";
    private final @NotNull String classPath = "com.microsoft.jdbc.sqlserver.SQLServerDriver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:microsoft:sqlserver://%s:%s;DatabaseName=%s", host, port, schema);
    }

}

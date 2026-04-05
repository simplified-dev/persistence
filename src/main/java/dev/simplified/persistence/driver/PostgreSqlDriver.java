package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for PostgreSQL databases (default port 5432).
 */
@Getter
public class PostgreSqlDriver implements JpaDriver {

    private final int defaultPort = 5432;
    private final @NotNull String dialectClass = "org.hibernate.dialect.PostgreSQLDialect";
    private final @NotNull String classPath = "org.postgresql.Driver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:postgresql://%s:%s/%s", host, port, schema);
    }

}

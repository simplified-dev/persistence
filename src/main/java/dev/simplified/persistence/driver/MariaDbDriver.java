package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for MariaDB databases (default port 3306).
 */
@Getter
public class MariaDbDriver implements JpaDriver {

    private final int defaultPort = 3306;
    private final @NotNull String dialectClass = "org.hibernate.dialect.MariaDBDialect";
    private final @NotNull String classPath = "org.mariadb.jdbc.Driver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:mariadb://%s:%s/%s", host, port, schema);
    }

}

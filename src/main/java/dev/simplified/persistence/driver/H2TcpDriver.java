package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for H2 databases accessed over TCP (default port 9092).
 * <p>
 * Connects to an H2 server instance running in TCP mode, allowing remote access
 * to file-based or in-memory databases.
 */
@Getter
public class H2TcpDriver implements JpaDriver {

    private final int defaultPort = 9092;
    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:h2:tcp://%s:%s/%s", host, port, schema);
    }

}

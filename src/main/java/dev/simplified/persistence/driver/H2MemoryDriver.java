package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for H2 in-memory databases (default port 0).
 * <p>
 * Creates a temporary in-memory schema that is dropped when the session closes.
 * Does not require external connection details or a connection pool.
 */
@Getter
public class H2MemoryDriver implements JpaDriver {

    private final int defaultPort = 0;
    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", schema);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEmbedded() {
        return true;
    }

}

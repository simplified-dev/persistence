package dev.simplified.persistence.driver;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

/**
 * {@link JpaDriver} implementation for H2 file-based databases (default port 0).
 * <p>
 * Stores data in a persistent file on disk. The {@code schema} parameter is used as
 * the file path (e.g. {@code "./data/mydb"} produces {@code jdbc:h2:file:./data/mydb}).
 */
@Getter
public class H2FileDriver implements JpaDriver {

    private final int defaultPort = 0;
    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";

    /** {@inheritDoc} */
    @Override
    public @NotNull String getConnectionUrl(@NotNull String host, int port, @NotNull String schema) {
        return String.format("jdbc:h2:file:%s", schema);
    }

}

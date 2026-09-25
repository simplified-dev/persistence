package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;

/**
 * An H2 database held in a file on disk.
 *
 * <p>It is reached by path and by nothing else, and it outlives the session that opened it - so the
 * schema is brought up to the mapping on connect rather than created and dropped around it.
 */
@Getter
public final class H2FileDriver implements JpaDriver {

    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.UPDATE;

    private H2FileDriver() {}

    /**
     * Names a file database.
     *
     * @param path the database file path, without the H2 suffix
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder at(@NotNull Path path) {
        return RelationalSource.builder()
            .withDriver(new H2FileDriver())
            .withUrl(String.format("jdbc:h2:file:%s", path))
            .build();
    }

}

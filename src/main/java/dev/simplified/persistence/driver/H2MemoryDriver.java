package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * An H2 database held in memory, created on connect and dropped on close.
 *
 * <p>It is reached by name and by nothing else: there is no host to point at and no account to
 * authenticate as, which is why {@link #named} takes one argument.
 */
@Getter
public final class H2MemoryDriver implements JpaDriver {

    private final @NotNull String dialectClass = "org.hibernate.dialect.H2Dialect";
    private final @NotNull String classPath = "org.h2.Driver";
    private final @NotNull SchemaPolicy schemaPolicy = SchemaPolicy.CREATE_DROP;

    private H2MemoryDriver() {}

    /**
     * Names an in-memory database.
     *
     * <p>The delay on close is what keeps the database alive between connections within one session;
     * dropping it is the session factory closing, not the last connection returning.
     *
     * @param name the database name, unique within the JVM
     * @return a builder over that database
     */
    public static @NotNull RelationalSource.Builder named(@NotNull String name) {
        return RelationalSource.of(
            new H2MemoryDriver(),
            String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", name)
        );
    }

}

package dev.simplified.persistence.driver;

import dev.simplified.persistence.source.Connection;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;

/**
 * What kind of database a connection speaks to: its Hibernate dialect, its JDBC driver, and who owns
 * its schema.
 *
 * <p>Three constants and nothing else. Where a particular database is, and what credentials reach it,
 * belong to its {@link Connection} - and each implementation fills one in through a static of its own
 * that answers the {@link RelationalSource.Builder} it leads into, so an in-memory database has nowhere
 * to put a host and a networked one cannot be reached without its credentials.
 *
 * @see Connection
 * @see RelationalSource
 * @see SchemaPolicy
 */
public interface JpaDriver {

    /**
     * The fully-qualified name of the Hibernate dialect class for this database type.
     */
    @NotNull String getDialectClass();

    /**
     * The fully-qualified class path of the JDBC driver implementation for this database.
     */
    @NotNull String getClassPath();

    /**
     * Who owns the schema this driver connects to.
     */
    @NotNull SchemaPolicy getSchemaPolicy();

}

package dev.simplified.persistence.source;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.BuilderNames;
import dev.simplified.annotations.ClassBuilder;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.SetterNames;
import dev.simplified.persistence.driver.JpaDriver;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * Where a {@link RelationalSource} connects: the {@link JpaDriver} that speaks to the database, the
 * JDBC url reaching it, and the account it is reached with where it takes one.
 *
 * <p>A connection is the first thing a relational source is given. {@link RelationalSource#builder()}
 * answers a {@link Builder} and nothing else, and building it hands the connection to the
 * {@link RelationalSource.Builder} it leads into. A driver's own static fills the connection in and
 * answers that builder directly, so a caller reaching for a driver never renders a url.
 */
@Getter(AccessLevel.PACKAGE)
@ClassBuilder(
    setters = @SetterNames(set = "with{}"),
    builder = @BuilderNames(builder = BuilderNames.NONE, from = BuilderNames.NONE, toBuilder = BuilderNames.NONE)
)
public final class Connection {

    /**
     * What kind of database it is.
     */
    private final @NotNull JpaDriver driver;

    /**
     * The JDBC url reaching it, rendered by the driver that names it.
     */
    private final @NotNull String url;

    /**
     * The account it is reached with, empty for a database that takes none.
     */
    private final @NotNull Optional<Credentials> credentials = Optional.empty();

    /**
     * Constructs a connection to the database a driver and url name.
     *
     * @param driver what kind of database it is
     * @param url the JDBC url reaching it
     * @param credentials the account it is reached with, empty for none
     * @throws JpaException if no driver or no url is given
     */
    Connection(@Nullable JpaDriver driver, @Nullable String url, @NotNull Optional<Credentials> credentials) {
        if (driver == null)
            throw new JpaException("A connection names no driver");

        if (url == null)
            throw new JpaException("A connection names no url");

        this.driver = driver;
        this.url = url;
        this.credentials = credentials;
    }

    /**
     * Collects a connection, and hands it to the {@link RelationalSource.Builder} it leads into.
     */
    public static final class Builder {

        /**
         * The relational source builder the connection is handed to.
         */
        private final @NotNull RelationalSource.Builder source;

        /**
         * Constructs a builder leading into the given relational source builder.
         *
         * @param source the builder the connection is handed to
         */
        Builder(@NotNull RelationalSource.Builder source) {
            this.source = source;
        }

        /**
         * Builds the connection and hands it to the relational source builder it leads into.
         *
         * @return the relational source builder, holding this connection
         * @throws JpaException if no driver or no url is given
         */
        public @NotNull RelationalSource.Builder build() {
            return this.source.withConnection(new Connection(this.driver, this.url, this.credentials));
        }

    }

    /**
     * An account a database is reached with.
     *
     * <p>Whoever holds an opened database's Hibernate access can read it back: the service registry
     * behind its session factory and its boot metadata keep it, its connection pool keeps it, and a
     * MariaDB connection answers it. Hand the database only to code trusted with the account.
     */
    @Getter(AccessLevel.PACKAGE)
    public static final class Credentials {

        /**
         * The account name.
         */
        private final @NotNull String user;

        /**
         * The account password.
         */
        private final @NotNull String password;

        /**
         * Constructs the credentials of an account.
         *
         * @param user the account name
         * @param password the account password
         */
        public Credentials(@NotNull String user, @NotNull String password) {
            this.user = user;
            this.password = password;
        }

    }

}

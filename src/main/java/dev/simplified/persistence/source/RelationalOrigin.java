package dev.simplified.persistence.source;

import com.google.gson.Gson;
import dev.simplified.annotations.Getter;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.driver.JpaDriver;
import dev.simplified.persistence.driver.SchemaPolicy;
import dev.simplified.util.Logging;
import org.ehcache.core.Ehcache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

/**
 * Which database a session's rows live in: a driver, a rendered JDBC url, the credentials that reach
 * it, and the caching Hibernate does over it.
 *
 * <p>The relational counterpart of a {@link DocumentOrigin}, and the same question - where the store
 * is. It differs in needing no implementation, because JDBC is already the pluggable part: a driver
 * plus a url is the whole of the variation, so this is a value a caller builds rather than an
 * interface a caller writes.
 *
 * <p>A caller never renders the url. Each {@link JpaDriver} names the addresses it has through a
 * static of its own, so the shape of the address is decided by which driver was reached for rather
 * than by validating fields afterwards.
 *
 * @see DocumentOrigin
 * @see JpaDriver
 */
@Getter
public final class RelationalOrigin {

    /**
     * What kind of database this is.
     */
    private final @NotNull JpaDriver driver;

    /**
     * The JDBC url reaching it, rendered by the driver that named it.
     */
    private final @NotNull String url;

    /**
     * The credentials it is reached with, empty for a database that takes none.
     */
    private final @NotNull Optional<Credentials> credentials;

    private final boolean usingQueryCache;
    private final boolean using2ndLevelCache;
    private final boolean usingStatistics;
    private final @NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy;
    private final @NotNull CacheMissingStrategy missingCacheStrategy;
    private final long queryResultsTTL;
    private final long defaultCacheExpiryMs;
    private final @NotNull JpaCacheProvider cacheProvider;

    private RelationalOrigin(@NotNull Builder builder) {
        this.driver = builder.driver;
        this.url = builder.url;
        this.credentials = Optional.ofNullable(builder.credentials);
        this.usingQueryCache = builder.usingQueryCache;
        this.using2ndLevelCache = builder.using2ndLevelCache;
        this.usingStatistics = builder.usingStatistics;
        this.cacheConcurrencyStrategy = builder.cacheConcurrencyStrategy;
        this.missingCacheStrategy = builder.missingCacheStrategy;
        this.queryResultsTTL = builder.queryResultsTTL;
        this.defaultCacheExpiryMs = builder.defaultCacheExpiryMs;
        this.cacheProvider = builder.cacheProvider;
    }

    /**
     * Names a database a caller reaches without credentials.
     *
     * <p>Called by a {@link JpaDriver} that has rendered its url, never by a consumer.
     *
     * @param driver what kind of database it is
     * @param url the JDBC url reaching it
     * @return a builder over that database
     */
    public static @NotNull Builder of(@NotNull JpaDriver driver, @NotNull String url) {
        return new Builder(driver, url);
    }

    /**
     * Names a database that has to be authenticated before it can be addressed.
     *
     * <p>Called by a {@link JpaDriver} that has rendered its url, never by a consumer. The returned
     * step holds no database yet, which is what stops a caller reaching one without a password.
     *
     * @param driver what kind of database it is
     * @param url the JDBC url reaching it
     * @return the step that takes the credentials
     */
    public static @NotNull Authenticating authenticating(@NotNull JpaDriver driver, @NotNull String url) {
        return new Authenticating(driver, url);
    }

    /**
     * Whether reaching this database wants a connection pool.
     *
     * @return {@code true} when it is reached over a network rather than opened in place
     */
    public boolean isPooled() {
        return this.credentials.isPresent();
    }

    /**
     * Opens the database and holds it, so the rows of every type it maps can be read and written.
     *
     * <p>The mapped types are the database's, not a session's. A session registers the types it
     * holds a generation of, and a mapped type left out of that list is reached through the returned
     * source's Hibernate access instead.
     *
     * @param models the types the database maps
     * @param gson the parser custom Hibernate types bind through
     * @param logLevel the level the connection logs at
     * @return the open database, which the caller owns and must close once every session reading it
     *         is shut down
     */
    public @NotNull RelationalSource open(
        @NotNull ConcurrentList<Class<JpaModel>> models,
        @NotNull Gson gson,
        @NotNull Logging.Level logLevel
    ) {
        this.applyLogLevel(logLevel, models);
        return new RelationalSource(this, models, gson, logLevel);
    }

    /**
     * Applies a log level to every logger this database brings with it.
     *
     * @param level the level to apply
     * @param models the types whose cache regions log under their own names
     */
    private void applyLogLevel(@NotNull Logging.Level level, @NotNull ConcurrentList<Class<JpaModel>> models) {
        Logging.setLevel("org.jboss.logging", level);
        Logging.setLevel("org.hibernate", level);
        Logging.setLevel("org.ehcache", level);
        Logging.setLevel(this.getDriver().getClassPath(), level);
        Logging.setLevel(String.format("%s-%s", Ehcache.class, "default-update-timestamps-region"), level);
        Logging.setLevel(String.format("%s-%s", Ehcache.class, "default-query-results-region"), level);
        models.forEach(model -> Logging.setLevel(String.format("%s-%s", Ehcache.class, model.getName()), level));

        if (this.isPooled())
            Logging.setLevel("com.zaxxer.hikari", level);
    }

    /**
     * The url and the schema policy, for a message naming what a session connected to.
     */
    @Override
    public @NotNull String toString() {
        return String.format("%s (%s)", this.url, this.driver.getSchemaPolicy());
    }

    /**
     * What a database is reached with.
     *
     * @param user the account name
     * @param password the account password
     */
    public record Credentials(@NotNull String user, @NotNull String password) {}

    /**
     * A database named but not yet authenticated.
     *
     * <p>It is not a {@link RelationalOrigin} and cannot become one without {@link #as}, which is how
     * a database that refuses anonymous connections refuses them at the point one is described rather
     * than at the point one is opened.
     */
    public static final class Authenticating {

        private final @NotNull JpaDriver driver;
        private final @NotNull String url;

        private Authenticating(@NotNull JpaDriver driver, @NotNull String url) {
            this.driver = driver;
            this.url = url;
        }

        /**
         * Names the account the database is reached with.
         *
         * @param user the account name
         * @param password the account password
         * @return a builder over that database
         */
        public @NotNull Builder as(@NotNull String user, @NotNull String password) {
            Builder builder = new Builder(this.driver, this.url);
            builder.credentials = new Credentials(user, password);
            return builder;
        }

    }

    /**
     * Names what Hibernate caches over a database.
     *
     * <p>An origin only exists where a database does, so the defaults are the ones a database wants:
     * both caches on, read-write concurrency, a missing region created with a warning, and a
     * thirty-second query result life.
     */
    public static final class Builder {

        private final @NotNull JpaDriver driver;
        private final @NotNull String url;
        private @Nullable Credentials credentials;

        private boolean usingQueryCache = true;
        private boolean using2ndLevelCache = true;
        private boolean usingStatistics = false;
        private @NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy = CacheConcurrencyStrategy.READ_WRITE;
        private @NotNull CacheMissingStrategy missingCacheStrategy = CacheMissingStrategy.CREATE_WARN;
        private long queryResultsTTL = 30;
        private long defaultCacheExpiryMs = 30_000;
        private @NotNull JpaCacheProvider cacheProvider = JpaCacheProvider.EHCACHE;

        private Builder(@NotNull JpaDriver driver, @NotNull String url) {
            this.driver = driver;
            this.url = url;
        }

        /**
         * Sets whether the Hibernate query cache is enabled.
         */
        public @NotNull Builder isUsingQueryCache(boolean value) {
            this.usingQueryCache = value;
            return this;
        }

        /**
         * Sets whether the Hibernate second-level cache is enabled.
         */
        public @NotNull Builder isUsing2ndLevelCache(boolean value) {
            this.using2ndLevelCache = value;
            return this;
        }

        /**
         * Enables Hibernate statistics gathering.
         */
        public @NotNull Builder isUsingStatistics() {
            this.usingStatistics = true;
            return this;
        }

        /**
         * Sets the Hibernate {@link CacheConcurrencyStrategy} for entity caching.
         */
        public @NotNull Builder withCacheConcurrencyStrategy(@NotNull CacheConcurrencyStrategy strategy) {
            this.cacheConcurrencyStrategy = strategy;
            return this;
        }

        /**
         * Sets the Hibernate {@link CacheMissingStrategy} for absent cache regions.
         */
        public @NotNull Builder withCacheMissingStrategy(@NotNull CacheMissingStrategy strategy) {
            this.missingCacheStrategy = strategy;
            return this;
        }

        /**
         * Sets the query results cache time-to-live in seconds.
         */
        public @NotNull Builder withQueryResultsTTL(long seconds) {
            this.queryResultsTTL = seconds;
            return this;
        }

        /**
         * Sets the default JCache TTL in milliseconds for types declaring no hydration cadence.
         */
        public @NotNull Builder withDefaultCacheExpiryMs(long defaultCacheExpiryMs) {
            this.defaultCacheExpiryMs = defaultCacheExpiryMs;
            return this;
        }

        /**
         * Sets the {@link JpaCacheProvider} backing the JCache second-level cache.
         */
        public @NotNull Builder withCacheProvider(@NotNull JpaCacheProvider cacheProvider) {
            this.cacheProvider = cacheProvider;
            return this;
        }

        /**
         * Builds the origin.
         *
         * <p>No connection is opened: {@link RelationalOrigin#open} opens it, because opening one
         * needs the types it maps.
         *
         * @return the database this describes
         */
        public @NotNull RelationalOrigin build() {
            return new RelationalOrigin(this);
        }

    }

    /**
     * The policy this database's schema is under.
     *
     * @return the schema policy of the driver it speaks through
     */
    public @NotNull SchemaPolicy getSchemaPolicy() {
        return this.driver.getSchemaPolicy();
    }

}

package dev.simplified.persistence;

import dev.simplified.persistence.driver.JpaDriver;
import dev.simplified.persistence.driver.MariaDbDriver;
import dev.simplified.persistence.source.Source;
import dev.simplified.util.LogUtil;
import dev.simplified.gson.GsonSettings;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.builder.BuildFlag;
import dev.simplified.util.SystemUtil;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.Level;
import org.ehcache.core.Ehcache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/**
 * Immutable configuration class for JPA sessions, constructed via its nested {@link Builder}.
 *
 * <p>Holds connection details, cache settings, and {@link GsonSettings} configuration.
 * The {@link JpaDriver} controls the database connection mode (external RDBMS,
 * embedded H2, etc.), while data-source concerns (JSON vs SQL) and entity discovery
 * are expressed through the {@link RepositoryFactory} and its {@link Source}
 * registrations.</p>
 *
 * <p>Use {@link #commonSql()} for a ready-made MariaDB preset, {@link #common(JpaDriver, String)}
 * for a pre-filled {@link Builder} with sensible cache defaults, or {@link #builder()} for
 * full control.</p>
 *
 * @see Builder
 * @see JpaSession
 * @see JpaDriver
 * @see RepositoryFactory
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JpaConfig {

    private final @NotNull UUID uniqueId = UUID.randomUUID();
    private final @NotNull JpaDriver driver;
private final @NotNull String host;
private final int port;
private final @NotNull String schema;
private final @NotNull String user;
private final @NotNull String password;
    private final boolean usingQueryCache;
    private final boolean using2ndLevelCache;
    private final boolean usingStatistics;
    private final @NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy;
    private final @NotNull CacheMissingStrategy missingCacheStrategy;
    private final long queryResultsTTL;
    private final long defaultCacheExpiryMs;
    private final @NotNull GsonSettings gsonSettings;
    private final @NotNull RepositoryFactory repositoryFactory;
    private @NotNull Level logLevel = Level.WARN;

    /**
     * Checks whether the current log level includes the given level.
     *
     * <p>Returns {@code true} when this config's log level is equal to or more verbose than
     * {@code level} (i.e. its {@code intLevel} is greater than or equal to the given level's).</p>
     *
     * @param level the log level to compare against
     * @return {@code true} if the current log level encompasses {@code level}
     */
    public boolean isLogLevel(@NotNull Level level) {
        return this.logLevel.intLevel() >= level.intLevel();
    }

    /**
     * Returns a new {@link Builder} for constructing a {@link JpaConfig} instance.
     *
     * @return a new builder with default values
     */
    public static @NotNull Builder builder() {
        return new Builder();
    }

    /**
     * Returns a pre-filled {@link Builder} with the given driver, schema, and sensible cache
     * defaults (L2 cache, query cache, {@code READ_WRITE} concurrency, 30-second query TTL).
     *
     * @param driver the database driver
     * @param schema the schema name
     * @return a pre-filled builder
     */
    public static @NotNull Builder common(@NotNull JpaDriver driver, @NotNull String schema) {
        return common(driver, Optional.of(schema));
    }

    /**
     * Returns a pre-filled {@link Builder} with the given driver, schema, and sensible cache
     * defaults (L2 cache, query cache, {@code READ_WRITE} concurrency, 30-second query TTL).
     *
     * @param driver the database driver
     * @param schema the schema name, or empty to require a later {@link Builder#withSchema(String)} call
     * @return a pre-filled builder
     */
    public static @NotNull Builder common(@NotNull JpaDriver driver, @NotNull Optional<String> schema) {
        return builder()
            .withDriver(driver)
            .withSchema(schema)
            .withLogLevel(Level.WARN)
            .isUsingQueryCache()
            .isUsing2ndLevelCache()
            .withCacheConcurrencyStrategy(CacheConcurrencyStrategy.READ_WRITE)
            .withCacheMissingStrategy(CacheMissingStrategy.CREATE_WARN)
            .withQueryResultsTTL(30);
    }

    /**
     * Builds a default MariaDB configuration using the {@code DATABASE_HOST} environment
     * variable as the schema name, with sensible cache defaults.
     *
     * @return a fully constructed MariaDB configuration
     */
    public static @NotNull JpaConfig commonSql() {
        return common(new MariaDbDriver(), SystemUtil.getEnv("DATABASE_HOST")).build();
    }

    /**
     * Sets the log level and propagates it to all underlying loggers.
     *
     * <p>Affected loggers include Hibernate, EhCache, JBoss Logging, Logback, the JDBC driver,
     * and per-entity cache regions. In SQL mode, the HikariCP logger is also updated.</p>
     *
     * <p>Delegates to {@link LogUtil} for backend-agnostic level propagation. Works with
     * both Log4j2 Core and Logback (via {@code log4j-to-slf4j} bridge).</p>
     *
     * @param logLevel the new log level to apply
     */
    public void setLogLevel(@NotNull Level logLevel) {
        this.logLevel = logLevel;
        String levelName = logLevel.name();
        LogUtil.setLevel("org.jboss.logging", levelName);
        LogUtil.setLevel("ch.qos.logback", levelName);
        LogUtil.setLevel("org.hibernate", levelName);
        LogUtil.setLevel("org.ehcache", levelName);
        LogUtil.setLevel(this.getDriver().getClassPath(), levelName);
        LogUtil.setLevel(String.format("%s-%s", Ehcache.class, "default-update-timestamps-region"), levelName);
        LogUtil.setLevel(String.format("%s-%s", Ehcache.class, "default-query-results-region"), levelName);
        this.getRepositoryFactory().getModels().forEach(model -> LogUtil.setLevel(String.format("%s-%s", Ehcache.class, model.getName()), levelName));

        if (!this.getDriver().isEmbedded())
            LogUtil.setLevel("com.zaxxer.hikari", levelName);
    }

    /**
     * Fluent builder for constructing {@link JpaConfig} instances.
     *
     * <p>All fields carry sensible defaults. The {@code schema} name is always required.
     * For non-embedded drivers, connection fields ({@code host}, {@code port}, {@code user},
     * {@code password}) are also validated at {@link #build()} time. For embedded drivers
     * (when {@link JpaDriver#isEmbedded()} returns {@code true}), those connection fields
     * are optional and ignored.</p>
     *
     * @see JpaConfig#builder()
     */
    public static class Builder {

        @BuildFlag(nonNull = true)
        private JpaDriver driver = new MariaDbDriver();
        private Optional<String> host = Optional.empty();
        private Optional<Integer> port = Optional.empty();
        private Optional<String> schema = Optional.empty();
        private Optional<String> user = Optional.empty();
        private Optional<String> password = Optional.empty();

        @BuildFlag(nonNull = true)
        private Level logLevel = Level.WARN;
        private boolean usingQueryCache = false;
        private boolean using2ndLevelCache = false;
        private boolean usingStatistics = false;
        @BuildFlag(nonNull = true)
        private CacheConcurrencyStrategy cacheConcurrencyStrategy = CacheConcurrencyStrategy.NONE;
        @BuildFlag(nonNull = true)
        private CacheMissingStrategy missingCacheStrategy = CacheMissingStrategy.FAIL;
        @BuildFlag(nonNull = true)
        private long queryResultsTTL = 0;
        private long defaultCacheExpiryMs = 30_000;
        private GsonSettings gsonSettings = GsonSettings.builder().build();
        private @Nullable RepositoryFactory repositoryFactory;

        /** Enables the Hibernate second-level cache. */
        public Builder isUsing2ndLevelCache() {
            return this.isUsing2ndLevelCache(true);
        }

        /** Sets whether the Hibernate second-level cache is enabled. */
        public Builder isUsing2ndLevelCache(boolean value) {
            this.using2ndLevelCache = value;
            return this;
        }

        /** Enables the Hibernate query cache. */
        public Builder isUsingQueryCache() {
            return this.isUsingQueryCache(true);
        }

        /** Sets whether the Hibernate query cache is enabled. */
        public Builder isUsingQueryCache(boolean value) {
            this.usingQueryCache = value;
            return this;
        }

        /** Enables Hibernate statistics gathering. */
        public Builder isUsingStatistics() {
            return this.isUsingStatistics(true);
        }

        /** Sets whether Hibernate statistics gathering is enabled. */
        public Builder isUsingStatistics(boolean value) {
            this.usingStatistics = value;
            return this;
        }

        /** Sets the Hibernate {@link CacheConcurrencyStrategy} for entity caching. */
        public Builder withCacheConcurrencyStrategy(@NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy) {
            this.cacheConcurrencyStrategy = cacheConcurrencyStrategy;
            return this;
        }

        /** Sets the {@link JpaDriver} used for database connectivity. */
        public Builder withDriver(@NotNull JpaDriver driver) {
            this.driver = driver;
            return this;
        }

        /** Sets the {@link GsonSettings} used for JSON serialization within the session. */
        public Builder withGsonSettings(@NotNull GsonSettings gsonSettings) {
            this.gsonSettings = gsonSettings;
            return this;
        }

        /** Sets the factory used to create {@link JpaRepository} instances during {@link JpaSession#cacheRepositories()}. */
        public Builder withRepositoryFactory(@NotNull RepositoryFactory repositoryFactory) {
            this.repositoryFactory = repositoryFactory;
            return this;
        }

        /** Sets the database host address. */
        public Builder withHost(@NotNull String host) {
            return this.withHost(Optional.of(host));
        }

        /** Sets the database host address from an {@link Optional}. */
        public Builder withHost(@NotNull Optional<String> host) {
            this.host = host;
            return this;
        }

        /** Sets the log level applied at {@link #build()} time. */
        public Builder withLogLevel(@NotNull Level level) {
            this.logLevel = level;
            return this;
        }

        /** Sets the Hibernate {@link CacheMissingStrategy} for absent cache regions. */
        public Builder withCacheMissingStrategy(@NotNull CacheMissingStrategy missingCacheStrategy) {
            this.missingCacheStrategy = missingCacheStrategy;
            return this;
        }

        /** Sets the database password. */
        public Builder withPassword(@NotNull String password) {
            return this.withPassword(Optional.of(password));
        }

        /** Sets the database password from an {@link Optional}. */
        public Builder withPassword(@NotNull Optional<String> password) {
            this.password = password;
            return this;
        }

        /** Sets the database port number. */
        public Builder withPort(int port) {
            return this.withPort(Optional.of(port));
        }

        /** Sets the database port number from an {@link Optional}. */
        public Builder withPort(@NotNull Optional<Integer> port) {
            this.port = port;
            return this;
        }

        /** Sets the query results cache time-to-live in seconds. */
        public Builder withQueryResultsTTL(long queryResultsTTL) {
            this.queryResultsTTL = queryResultsTTL;
            return this;
        }

        /** Sets the default JCache TTL in milliseconds for entity types without a {@link CacheExpiry} annotation. */
        public Builder withDefaultCacheExpiryMs(long defaultCacheExpiryMs) {
            this.defaultCacheExpiryMs = defaultCacheExpiryMs;
            return this;
        }

        /** Sets the database schema name. */
        public Builder withSchema(@NotNull String schema) {
            return this.withSchema(Optional.of(schema));
        }

        /** Sets the database schema name from an {@link Optional}. */
        public Builder withSchema(@NotNull Optional<String> schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the database username. */
        public Builder withUser(@NotNull String user) {
            return this.withUser(Optional.of(user));
        }

        /** Sets the database username from an {@link Optional}. */
        public Builder withUser(@NotNull Optional<String> user) {
            this.user = user;
            return this;
        }

        /**
         * Validates builder flags, resolves defaults, and constructs the {@link JpaConfig}.
         *
         * <p>A schema name is always required. For non-embedded drivers, all connection
         * fields ({@code host}, {@code port}, {@code user}, {@code password}) must also
         * be present or an {@link IllegalStateException} is thrown.</p>
         *
         * @return a fully constructed, immutable {@link JpaConfig}
         * @throws IllegalStateException if required fields are missing
         */
        public @NotNull JpaConfig build() {
            Reflection.validateFlags(this);

            if (this.schema.isEmpty())
                throw new IllegalStateException("schema is required");

            this.checkEmbedded();

            JpaConfig jpaConfig = new JpaConfig(
                this.driver,
                this.host.orElse(""),
                this.port.orElse(this.driver.getDefaultPort()),
                this.schema.orElseThrow(),
                this.user.orElse(""),
                this.password.orElse(""),
                this.usingQueryCache,
                this.using2ndLevelCache,
                this.usingStatistics,
                this.cacheConcurrencyStrategy,
                this.missingCacheStrategy,
                this.queryResultsTTL,
                this.defaultCacheExpiryMs,
                this.gsonSettings,
                this.repositoryFactory != null ? this.repositoryFactory : RepositoryFactory.builder().build()
            );

            jpaConfig.setLogLevel(this.logLevel);
            return jpaConfig;
        }

        /**
         * Validates that connection fields are present for non-embedded drivers.
         *
         * @throws IllegalStateException if the driver is not embedded and any connection field is empty
         */
        private void checkEmbedded() {
            if (this.driver.isEmbedded())
                return;

            if (this.host.isEmpty()) throw new IllegalStateException("host is required for non-embedded drivers");
            if (this.port.isEmpty()) throw new IllegalStateException("port is required for non-embedded drivers");
            if (this.user.isEmpty()) throw new IllegalStateException("user is required for non-embedded drivers");
            if (this.password.isEmpty()) throw new IllegalStateException("password is required for non-embedded drivers");
        }

    }

}

package dev.simplified.persistence;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.driver.JpaDriver;
import dev.simplified.persistence.driver.MariaDbDriver;
import dev.simplified.persistence.store.Source;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.builder.BuildFlag;
import dev.simplified.util.Logging;
import dev.simplified.util.SystemUtil;
import org.ehcache.core.Ehcache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/**
 * Immutable configuration class for JPA sessions, constructed via its nested {@link Builder}.
 *
 * <p>Holds connection details, cache settings, and {@link GsonSettings} configuration. Entity
 * discovery and where each type's rows come from are expressed through the
 * {@link RepositoryFactory} and its {@link Source}.</p>
 *
 * <p>The {@link JpaDriver} is the gate. Present, it names a database and {@link JpaSession} builds
 * the whole relational stack around it - service registry, metadata, session factory, JCache
 * regions. Absent, none of that runs, the connection and cache fields below carry no meaning, and a
 * session serves types whose rows a {@link Source} supplies.</p>
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

    /**
     * The database this session connects to, empty when it has none.
     */
    private final @NotNull Optional<JpaDriver> driver;
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
    private final @NotNull JpaCacheProvider cacheProvider;
    private final @NotNull GsonSettings gsonSettings;
    private final @NotNull RepositoryFactory repositoryFactory;
    private @NotNull Logging.Level logLevel = Logging.Level.WARN;

    /**
     * Checks whether the current log level includes the given level.
     *
     * <p>Returns {@code true} when this config's log level is equal to or more verbose than
     * {@code level}.</p>
     *
     * @param level the log level to compare against
     * @return {@code true} if the current log level encompasses {@code level}
     */
    public boolean isLogLevel(@NotNull Logging.Level level) {
        return this.logLevel.includes(level);
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
            .withLogLevel(Logging.Level.WARN)
            .isUsingQueryCache()
            .isUsing2ndLevelCache()
            .withCacheConcurrencyStrategy(CacheConcurrencyStrategy.READ_WRITE)
            .withCacheMissingStrategy(CacheMissingStrategy.CREATE_WARN)
            .withQueryResultsTTL(30);
    }

    /**
     * Builds a MariaDB configuration from the {@code DATABASE_HOST}, {@code DATABASE_PORT},
     * {@code DATABASE_SCHEMA}, {@code DATABASE_USER} and {@code DATABASE_PASSWORD} environment
     * variables, with sensible cache defaults.
     *
     * @return a fully constructed MariaDB configuration
     * @throws IllegalStateException if any of the connection variables is unset
     */
    public static @NotNull JpaConfig commonSql() {
        return common(new MariaDbDriver(), SystemUtil.getEnv("DATABASE_SCHEMA"))
            .withHost(SystemUtil.getEnv("DATABASE_HOST"))
            .withPort(SystemUtil.getEnv("DATABASE_PORT").map(Integer::parseInt))
            .withUser(SystemUtil.getEnv("DATABASE_USER"))
            .withPassword(SystemUtil.getEnv("DATABASE_PASSWORD"))
            .build();
    }

    /**
     * Sets the log level and propagates it to all underlying loggers.
     *
     * <p>Affected loggers include JBoss Logging and Logback always, and - with a {@link JpaDriver}
     * present - Hibernate, EhCache, the JDBC driver, the per-entity cache regions and, for a
     * non-embedded driver, HikariCP.</p>
     *
     * <p>Delegates to {@link Logging} for backend-agnostic level propagation.</p>
     *
     * @param logLevel the new log level to apply
     */
    public void setLogLevel(@NotNull Logging.Level logLevel) {
        this.logLevel = logLevel;
        Logging.setLevel("org.jboss.logging", logLevel);
        Logging.setLevel("ch.qos.logback", logLevel);

        this.getDriver().ifPresent(driver -> {
            Logging.setLevel("org.hibernate", logLevel);
            Logging.setLevel("org.ehcache", logLevel);
            Logging.setLevel(driver.getClassPath(), logLevel);
            Logging.setLevel(String.format("%s-%s", Ehcache.class, "default-update-timestamps-region"), logLevel);
            Logging.setLevel(String.format("%s-%s", Ehcache.class, "default-query-results-region"), logLevel);
            this.getRepositoryFactory().getModels().forEach(model -> Logging.setLevel(String.format("%s-%s", Ehcache.class, model.getName()), logLevel));

            if (!driver.isEmbedded())
                Logging.setLevel("com.zaxxer.hikari", logLevel);
        });
    }

    /**
     * Fluent builder for constructing {@link JpaConfig} instances.
     *
     * <p>All fields carry sensible defaults, and {@code driver} is the one that decides what the
     * rest mean. Without one nothing else is validated and the session builds no relational stack.
     * With one, a {@code schema} name is required, and for a non-embedded driver so are
     * {@code host}, {@code port}, {@code user} and {@code password}. For an embedded driver
     * (when {@link JpaDriver#isEmbedded()} returns {@code true}) those connection fields are
     * optional and ignored.</p>
     *
     * @see JpaConfig#builder()
     */
    public static class Builder {

        private Optional<JpaDriver> driver = Optional.empty();
        private Optional<String> host = Optional.empty();
        private Optional<Integer> port = Optional.empty();
        private Optional<String> schema = Optional.empty();
        private Optional<String> user = Optional.empty();
        private Optional<String> password = Optional.empty();

        @BuildFlag(nonNull = true)
        private Logging.Level logLevel = Logging.Level.WARN;
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
        @BuildFlag(nonNull = true)
        private JpaCacheProvider cacheProvider = JpaCacheProvider.EHCACHE;
        private GsonSettings gsonSettings = GsonSettings.defaults();
        private @Nullable RepositoryFactory repositoryFactory;

        /**
         * Enables the Hibernate second-level cache.
         */
        public Builder isUsing2ndLevelCache() {
            return this.isUsing2ndLevelCache(true);
        }

        /**
         * Sets whether the Hibernate second-level cache is enabled.
         */
        public Builder isUsing2ndLevelCache(boolean value) {
            this.using2ndLevelCache = value;
            return this;
        }

        /**
         * Enables the Hibernate query cache.
         */
        public Builder isUsingQueryCache() {
            return this.isUsingQueryCache(true);
        }

        /**
         * Sets whether the Hibernate query cache is enabled.
         */
        public Builder isUsingQueryCache(boolean value) {
            this.usingQueryCache = value;
            return this;
        }

        /**
         * Enables Hibernate statistics gathering.
         */
        public Builder isUsingStatistics() {
            return this.isUsingStatistics(true);
        }

        /**
         * Sets whether Hibernate statistics gathering is enabled.
         */
        public Builder isUsingStatistics(boolean value) {
            this.usingStatistics = value;
            return this;
        }

        /**
         * Sets the Hibernate {@link CacheConcurrencyStrategy} for entity caching.
         */
        public Builder withCacheConcurrencyStrategy(@NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy) {
            this.cacheConcurrencyStrategy = cacheConcurrencyStrategy;
            return this;
        }

        /**
         * Sets the {@link JpaDriver} used for database connectivity, which is what makes the built
         * session relational.
         */
        public Builder withDriver(@NotNull JpaDriver driver) {
            this.driver = Optional.of(driver);
            return this;
        }

        /**
         * Sets the {@link GsonSettings} used for JSON serialization within the session.
         */
        public Builder withGsonSettings(@NotNull GsonSettings gsonSettings) {
            this.gsonSettings = gsonSettings;
            return this;
        }

        /**
         * Sets the factory used to create {@link JpaRepository} instances during {@link JpaSession#cacheRepositories()}.
         */
        public Builder withRepositoryFactory(@NotNull RepositoryFactory repositoryFactory) {
            this.repositoryFactory = repositoryFactory;
            return this;
        }

        /**
         * Sets the database host address.
         */
        public Builder withHost(@NotNull String host) {
            return this.withHost(Optional.of(host));
        }

        /**
         * Sets the database host address from an {@link Optional}.
         */
        public Builder withHost(@NotNull Optional<String> host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the log level applied at {@link #build()} time.
         */
        public Builder withLogLevel(@NotNull Logging.Level level) {
            this.logLevel = level;
            return this;
        }

        /**
         * Sets the Hibernate {@link CacheMissingStrategy} for absent cache regions.
         */
        public Builder withCacheMissingStrategy(@NotNull CacheMissingStrategy missingCacheStrategy) {
            this.missingCacheStrategy = missingCacheStrategy;
            return this;
        }

        /**
         * Sets the database password.
         */
        public Builder withPassword(@NotNull String password) {
            return this.withPassword(Optional.of(password));
        }

        /**
         * Sets the database password from an {@link Optional}.
         */
        public Builder withPassword(@NotNull Optional<String> password) {
            this.password = password;
            return this;
        }

        /**
         * Sets the database port number.
         */
        public Builder withPort(int port) {
            return this.withPort(Optional.of(port));
        }

        /**
         * Sets the database port number from an {@link Optional}.
         */
        public Builder withPort(@NotNull Optional<Integer> port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the query results cache time-to-live in seconds.
         */
        public Builder withQueryResultsTTL(long queryResultsTTL) {
            this.queryResultsTTL = queryResultsTTL;
            return this;
        }

        /**
         * Sets the default JCache TTL in milliseconds for entity types without a {@link CacheExpiry} annotation.
         */
        public Builder withDefaultCacheExpiryMs(long defaultCacheExpiryMs) {
            this.defaultCacheExpiryMs = defaultCacheExpiryMs;
            return this;
        }

        /**
         * Sets the {@link JpaCacheProvider} backing the JCache (JSR-107) second-level cache.
         */
        public Builder withCacheProvider(@NotNull JpaCacheProvider cacheProvider) {
            this.cacheProvider = cacheProvider;
            return this;
        }

        /**
         * Sets the database schema name.
         */
        public Builder withSchema(@NotNull String schema) {
            return this.withSchema(Optional.of(schema));
        }

        /**
         * Sets the database schema name from an {@link Optional}.
         */
        public Builder withSchema(@NotNull Optional<String> schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Sets the database username.
         */
        public Builder withUser(@NotNull String user) {
            return this.withUser(Optional.of(user));
        }

        /**
         * Sets the database username from an {@link Optional}.
         */
        public Builder withUser(@NotNull Optional<String> user) {
            this.user = user;
            return this;
        }

        /**
         * Validates builder flags, resolves defaults, and constructs the {@link JpaConfig}.
         *
         * <p>Nothing is required without a driver, because nothing else in this builder describes
         * anything then. With one, a schema name is required, and for a non-embedded driver so are
         * {@code host}, {@code port}, {@code user} and {@code password}, or an
         * {@link IllegalStateException} is thrown.</p>
         *
         * @return a fully constructed, immutable {@link JpaConfig}
         * @throws IllegalStateException if required fields are missing
         */
        public @NotNull JpaConfig build() {
            Reflection.validateFlags(this);
            this.driver.ifPresent(this::checkConnection);

            JpaConfig jpaConfig = new JpaConfig(
                this.driver,
                this.host.orElse(""),
                this.port.orElse(this.driver.map(JpaDriver::getDefaultPort).orElse(0)),
                this.schema.orElse(""),
                this.user.orElse(""),
                this.password.orElse(""),
                this.usingQueryCache,
                this.using2ndLevelCache,
                this.usingStatistics,
                this.cacheConcurrencyStrategy,
                this.missingCacheStrategy,
                this.queryResultsTTL,
                this.defaultCacheExpiryMs,
                this.cacheProvider,
                this.gsonSettings,
                this.repositoryFactory != null ? this.repositoryFactory : RepositoryFactory.of(JpaModel.class)
            );

            jpaConfig.setLogLevel(this.logLevel);
            return jpaConfig;
        }

        /**
         * Validates the fields a database needs, which is a schema always and the connection
         * fields for a non-embedded driver.
         *
         * @param driver the driver the session will connect through
         * @throws IllegalStateException if the schema is empty, or the driver is not embedded and any
         *         connection field is empty
         */
        private void checkConnection(@NotNull JpaDriver driver) {
            if (this.schema.isEmpty())
                throw new IllegalStateException("schema is required for a driver");

            if (driver.isEmbedded())
                return;

            if (this.host.isEmpty()) throw new IllegalStateException("host is required for non-embedded drivers");
            if (this.port.isEmpty()) throw new IllegalStateException("port is required for non-embedded drivers");
            if (this.user.isEmpty()) throw new IllegalStateException("user is required for non-embedded drivers");
            if (this.password.isEmpty()) throw new IllegalStateException("password is required for non-embedded drivers");
        }

    }

}

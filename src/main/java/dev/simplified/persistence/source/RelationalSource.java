package dev.simplified.persistence.source;

import com.google.gson.Gson;
import dev.simplified.annotations.Cleanup;
import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.driver.JpaDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.type.TypeRegistrar;
import dev.simplified.reflection.Reflection;
import dev.simplified.util.Logging;
import jakarta.persistence.criteria.CriteriaQuery;
import org.ehcache.core.Ehcache;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.mapping.SimpleValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An open database, and the rows every type it maps are read and written through.
 *
 * <p>A database is named through the static of the {@link JpaDriver} that speaks to it, which answers
 * a {@link Builder} - or, for a database that refuses anonymous connections, an {@link Authenticating}
 * step that only becomes one once credentials are given - and opened by {@link Builder#open}. A caller
 * never renders a url: the shape of the address is decided by which driver was reached for.
 *
 * <p>Everything Hibernate needs to exist is here and nowhere else - the service registry, the
 * metadata, the session factory and the JCache regions - so a session reading a document source holds
 * none of it. It is a {@link Source.Writable} like any other origin, which is what lets a repository
 * read a table the same way it reads a document.
 *
 * <p>Whoever opens one holds it: for the Hibernate access below, for the session it is handed to, and
 * to close it once every session reading it is shut down.
 *
 * <p>A relational origin always accepts writes. Refusing one is the database's job, through the
 * permissions the connection was opened under, rather than this library's.
 *
 * @see JpaDriver
 */
public final class RelationalSource implements Source.Writable, AutoCloseable {

    /**
     * JCache TTL is set to this multiple of the refresh interval as a safety net. Under normal
     * operation the scheduler refreshes proactively; the JCache TTL only fires if the scheduler
     * misses multiple cycles.
     */
    private static final int CACHE_TTL_MULTIPLIER = 2;

    private static final @NotNull String TIMESTAMPS_REGION = "default-update-timestamps-region";
    private static final @NotNull String QUERY_RESULTS_REGION = "default-query-results-region";

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

    /**
     * The types it maps.
     */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /**
     * Hibernate entity metadata including custom type registrations and column adjustments.
     */
    @Getter private final @NotNull Metadata metadata;

    /**
     * The Hibernate service registry backing {@link #sessionFactory}.
     */
    private final @NotNull StandardServiceRegistry serviceRegistry;

    /**
     * The Hibernate session factory opened from {@link #metadata}.
     */
    @Getter private final @NotNull SessionFactory sessionFactory;

    private RelationalSource(
        @NotNull Builder builder,
        @NotNull ConcurrentList<Class<JpaModel>> models,
        @NotNull Gson gson,
        @NotNull Logging.Level logLevel
    ) {
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
        this.models = models;

        this.applyLogLevel(logLevel);
        this.requireDriverOnClasspath();

        // The query-results and update-timestamps regions are only created when query caching is
        // actually enabled - a HAZELCAST_* provider disables it unconditionally, so creating these
        // would leave empty never-used JCache caches sitting on the cluster.
        if (this.isQueryCacheEnabled()) {
            this.buildCacheConfiguration(TIMESTAMPS_REGION, Duration.ETERNAL);
            this.buildCacheConfiguration(
                QUERY_RESULTS_REGION,
                this.queryResultsTTL <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.SECONDS, this.queryResultsTTL)
            );
        }

        this.serviceRegistry = new StandardServiceRegistryBuilder()
            .applySettings(this.createProperties(logLevel))
            .build();
        this.metadata = this.createMetadata(this.createMetadataSources().getMetadataBuilder(), gson);
        this.sessionFactory = this.metadata.buildSessionFactory();
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

    /** {@inheritDoc} */
    @Override
    public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException {
        return this.with(hibernate -> {
            CriteriaQuery<T> query = hibernate.getCriteriaBuilder().createQuery(type);
            query.select(query.from(type));
            return Concurrent.newUnmodifiableList(hibernate.createQuery(query).getResultList());
        });
    }

    /** {@inheritDoc} */
    @Override
    public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
        if (request.rows().isEmpty())
            return;

        if (request.operation() == WriteRequest.Operation.DELETE) {
            this.transaction((Consumer<Session>) hibernate -> request.rows().forEach(hibernate::remove));
            return;
        }

        // upsertMultiple bypasses dirty checking entirely, which is what keeps a row carrying a
        // read-only join column from raising HHH000502 when its association is not loaded.
        try (StatelessSession stateless = this.sessionFactory.openStatelessSession()) {
            stateless.getTransaction().begin();
            stateless.upsertMultiple(request.rows());
            stateless.getTransaction().commit();
        } catch (JpaException jpaException) {
            throw jpaException;
        } catch (Exception exception) {
            throw new JpaException(exception, "Failed to write '%s'", request.type().getName());
        }
    }

    /**
     * Opens a new Hibernate session, which the caller owns and must close.
     *
     * @return a freshly opened session
     */
    public @NotNull Session openSession() {
        return this.sessionFactory.openSession();
    }

    /**
     * Opens a managed session, passes it to the consumer, and closes it.
     *
     * @param consumer the operation to perform with the session
     * @throws JpaException if the session cannot be opened or the operation fails
     */
    public void with(@NotNull Consumer<Session> consumer) {
        try {
            @Cleanup Session session = this.openSession();
            consumer.accept(session);
        } catch (Exception exception) {
            throw new JpaException(exception);
        }
    }

    /**
     * Opens a managed session, passes it to the function, returns the result, and closes it.
     *
     * @param function the operation to perform with the session
     * @param <R> the return type
     * @return the result produced by the function
     * @throws JpaException if the session cannot be opened or the operation fails
     */
    public <R> R with(@NotNull Function<Session, R> function) {
        try {
            @Cleanup Session session = this.openSession();
            return function.apply(session);
        } catch (Exception exception) {
            throw new JpaException(exception);
        }
    }

    /**
     * Opens a managed session, executes the consumer within a transaction, and closes it.
     *
     * @param consumer the transactional operation to perform
     * @throws JpaException if the session cannot be opened or the transaction fails
     */
    public void transaction(@NotNull Consumer<Session> consumer) {
        this.with(session -> {
            Transaction transaction = session.beginTransaction();
            consumer.accept(session);
            transaction.commit();
        });
    }

    /**
     * Opens a managed session, executes the function within a transaction, and closes it.
     *
     * @param function the transactional operation to perform
     * @param <R> the return type
     * @return the result produced by the function
     * @throws JpaException if the session cannot be opened or the transaction fails
     */
    public <R> R transaction(@NotNull Function<Session, R> function) {
        return this.with(session -> {
            Transaction transaction = session.beginTransaction();
            R result = function.apply(session);
            transaction.commit();
            return result;
        });
    }

    /**
     * Closes the session factory, which drops the schema a {@code create-drop} policy created,
     * destroys the service registry and removes every cache region this opened.
     */
    @Override
    public void close() {
        this.sessionFactory.close();
        StandardServiceRegistryBuilder.destroy(this.serviceRegistry);

        CacheManager cacheManager = this.resolveCacheManager();
        this.models.forEach(model -> destroyCache(cacheManager, model.getName()));

        // Match the conditional region creation in the constructor: skip destroy when the region was
        // never created, which is the Hazelcast path with query caching disabled.
        destroyCache(cacheManager, TIMESTAMPS_REGION);
        destroyCache(cacheManager, QUERY_RESULTS_REGION);
    }

    /**
     * The url and the schema policy, for a message naming what a caller opened.
     */
    @Override
    public @NotNull String toString() {
        return String.format("%s (%s)", this.url, this.driver.getSchemaPolicy());
    }

    /**
     * Applies a log level to every logger this database brings with it.
     *
     * @param level the level to apply
     */
    private void applyLogLevel(@NotNull Logging.Level level) {
        Logging.setLevel("org.jboss.logging", level);
        Logging.setLevel("org.hibernate", level);
        Logging.setLevel("org.ehcache", level);
        Logging.setLevel(this.driver.getClassPath(), level);
        Logging.setLevel(String.format("%s-%s", Ehcache.class, TIMESTAMPS_REGION), level);
        Logging.setLevel(String.format("%s-%s", Ehcache.class, QUERY_RESULTS_REGION), level);
        this.models.forEach(model -> Logging.setLevel(String.format("%s-%s", Ehcache.class, model.getName()), level));

        if (this.credentials.isPresent())
            Logging.setLevel("com.zaxxer.hikari", level);
    }

    /**
     * Whether Hibernate's query cache is active for this database.
     *
     * <p>A Hazelcast provider disables it unconditionally: the query results region wraps results in
     * a {@code Serializable} holder that Hazelcast routes through {@code ObjectOutputStream}, which
     * walks the object graph with no hook for its own serialization service. Nothing on the read path
     * consults the region, so disabling it removes the last code path that touches it.
     */
    private boolean isQueryCacheEnabled() {
        return this.usingQueryCache
            && this.cacheProvider != JpaCacheProvider.HAZELCAST_CLIENT
            && this.cacheProvider != JpaCacheProvider.HAZELCAST_EMBEDDED;
    }

    /**
     * Fails before Hibernate does when the JDBC driver this speaks through is not on the classpath.
     *
     * @throws JpaException if the driver class cannot be loaded
     */
    private void requireDriverOnClasspath() {
        String classPath = this.driver.getClassPath();

        try {
            Class.forName(classPath);
        } catch (ClassNotFoundException exception) {
            throw new JpaException(exception, "No JDBC driver '%s' on the classpath for '%s'", classPath, this);
        }
    }

    /**
     * Assembles Hibernate and HikariCP properties from this database's settings.
     *
     * @param logLevel the level the connection logs at
     */
    private @NotNull ConcurrentMap<String, Object> createProperties(@NotNull Logging.Level logLevel) {
        ConcurrentMap<String, Object> properties = Concurrent.newMap();

        this.credentials.ifPresent(credentials -> {
            properties.put("hibernate.connection.username", credentials.user());
            properties.put("hibernate.connection.password", credentials.password());
            properties.put("hibernate.connection.provider_class", "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");
            properties.put("hikari.maximumPoolSize", 20);
        });

        String hbm2ddl = this.driver.getSchemaPolicy().getHbm2ddl();

        if (hbm2ddl != null)
            properties.put("hibernate.hbm2ddl.auto", hbm2ddl);

        properties.put("hibernate.dialect", this.driver.getDialectClass());
        properties.put("hibernate.connection.driver_class", this.driver.getClassPath());
        properties.put("hibernate.connection.url", this.url);
        properties.put("hibernate.globally_quoted_identifiers", true);

        properties.put("hibernate.jdbc.log.warnings", logLevel.includes(Logging.Level.WARN));
        properties.put("hibernate.show_sql", logLevel.includes(Logging.Level.DEBUG));
        properties.put("hibernate.format_sql", logLevel.includes(Logging.Level.TRACE));
        properties.put("hibernate.highlight_sql", logLevel.includes(Logging.Level.TRACE));
        properties.put("hibernate.use_sql_comments", logLevel.includes(Logging.Level.DEBUG));

        properties.put("hibernate.generate_statistics", this.usingStatistics);

        properties.put("hibernate.order_inserts", true);
        properties.put("hibernate.order_updates", true);

        properties.put("hibernate.jdbc.batch_size", 100);
        properties.put("hibernate.jdbc.fetch_size", 400);
        properties.put("hibernate.jdbc.use_get_generated_keys", true);

        properties.put("hibernate.cache.region.factory_class", "jcache");
        properties.put("hibernate.cache.use_reference_entries", true);
        properties.put("hibernate.cache.use_structured_entries", logLevel.includes(Logging.Level.DEBUG));
        properties.put("hibernate.cache.use_query_cache", this.isQueryCacheEnabled());
        properties.put("hibernate.cache.use_second_level_cache", this.using2ndLevelCache);
        properties.put("hibernate.javax.cache.missing_cache_strategy", this.missingCacheStrategy.getExternalRepresentation());

        // Pin the JCache provider for Hibernate's internal JCacheRegionFactory so it does not call
        // the no-arg lookup, which throws when more than one provider sits on the runtime classpath.
        // Property names use the hibernate.javax.cache.* prefix per hibernate-jcache 7.3 - the
        // jakarta-prefixed equivalents are not honored.
        properties.put("hibernate.javax.cache.provider", this.cacheProvider.getProviderClassName());

        if (this.cacheProvider.getConfigUri() != null)
            properties.put("hibernate.javax.cache.uri", this.cacheProvider.getConfigUri());

        if (this.cacheConcurrencyStrategy != CacheConcurrencyStrategy.NONE)
            properties.put("hibernate.cache.default_cache_concurrency_strategy", this.cacheConcurrencyStrategy.toAccessType().getExternalName());

        return properties.toUnmodifiable();
    }

    /**
     * Registers the mapped entity classes, each with its own cache region.
     */
    private @NotNull MetadataSources createMetadataSources() {
        MetadataSources metadataSources = new MetadataSources(this.serviceRegistry);

        this.models
            .stream()
            .map(this::buildCacheConfiguration)
            .forEach(metadataSources::addAnnotatedClass);

        return metadataSources;
    }

    /**
     * Discovers type registrars, registers their types, builds the metadata and adjusts column
     * lengths for a schema this generates.
     *
     * @param metadataBuilder the builder to register custom types with
     * @param gson the parser custom Hibernate types bind through
     * @return the fully built and post-processed metadata
     */
    private @NotNull Metadata createMetadata(@NotNull MetadataBuilder metadataBuilder, @NotNull Gson gson) {
        ConcurrentList<TypeRegistrar> registrars = Reflection.getResources()
            .filterPackage(TypeRegistrar.class)
            .getSubtypesOf(TypeRegistrar.class)
            .stream()
            .filter(cls -> !cls.isInterface() && !Modifier.isAbstract(cls.getModifiers()))
            .map(cls -> (TypeRegistrar) new Reflection<>(cls).newInstance())
            .collect(Concurrent.toList());

        registrars.forEach(registrar -> {
            registrar.scan(gson, this.models);
            registrar.register(metadataBuilder);
        });

        Metadata metadata = metadataBuilder.build();
        registrars.forEach(registrar -> registrar.postProcess(metadata));

        if (this.driver.getSchemaPolicy().isGenerated())
            adjustColumnLength(metadata);

        return metadata;
    }

    /**
     * Creates a JCache configuration for one type, with the TTL its {@link Hydration} cadence or the
     * database's default asks for, multiplied as a safety net.
     */
    private @NotNull Class<JpaModel> buildCacheConfiguration(@NotNull Class<JpaModel> type) {
        Hydration hydration = type.getAnnotation(Hydration.class);
        long expiryMs = hydration != null && hydration.every() > 0
            ? hydration.unit().toMillis(hydration.every())
            : this.defaultCacheExpiryMs;

        long jcacheTtlMs = expiryMs <= 0 ? 0 : expiryMs * CACHE_TTL_MULTIPLIER;
        Duration duration = jcacheTtlMs <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.MILLISECONDS, jcacheTtlMs);
        this.buildCacheConfiguration(type.getName(), duration);
        return type;
    }

    /**
     * Creates a JCache configuration with the given name and TTL, reusing an existing region.
     */
    private void buildCacheConfiguration(@NotNull String cacheName, @NotNull Duration duration) {
        CacheManager cacheManager = this.resolveCacheManager();

        if (cacheManager.getCache(cacheName, Object.class, Object.class) != null)
            return;

        cacheManager.createCache(
            cacheName,
            new MutableConfiguration<>()
                .setStoreByValue(false)
                .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(duration))
        );
    }

    /**
     * Resolves the JCache manager for this database's provider, opening it against the provider's
     * configuration resource when it names one.
     *
     * @return the cache manager for the configured provider
     */
    private @NotNull CacheManager resolveCacheManager() {
        javax.cache.spi.CachingProvider cachingProvider = Caching.getCachingProvider(this.cacheProvider.getProviderClassName());

        if (this.cacheProvider.getConfigUri() == null)
            return cachingProvider.getCacheManager();

        try {
            return cachingProvider.getCacheManager(new URI(this.cacheProvider.getConfigUri()), cachingProvider.getDefaultClassLoader());
        } catch (URISyntaxException exception) {
            throw new JpaException(exception, "Invalid cache provider config URI '%s'", this.cacheProvider.getConfigUri());
        }
    }

    private static void destroyCache(@NotNull CacheManager cacheManager, @NotNull String name) {
        if (cacheManager.getCache(name, Object.class, Object.class) != null)
            cacheManager.destroyCache(name);
    }

    /**
     * Widens every default-length {@code VARCHAR} column to 1,000,000, preventing truncation of data
     * whose lengths are unpredictable.
     *
     * <p>Only asked of a schema this generates. Elsewhere the column sizes are the database's.
     *
     * @param metadata the built metadata whose column definitions are adjusted in place
     */
    private static void adjustColumnLength(@NotNull Metadata metadata) {
        for (PersistentClass pc : metadata.getEntityBindings()) {
            for (Property prop : pc.getProperties()) {
                if (prop.getValue() instanceof SimpleValue sv) {
                    for (Column col : sv.getColumns()) {
                        if (col.getLength() != null && col.getLength() == 255L)
                            col.setLength(1_000_000L);
                    }
                }
            }
        }
    }

    /**
     * What a database is reached with.
     *
     * @param user the account name
     * @param password the account password
     */
    private record Credentials(@NotNull String user, @NotNull String password) {}

    /**
     * A database named but not yet authenticated.
     *
     * <p>It is not a {@link Builder} and cannot become one without {@link #as}, which is how a
     * database that refuses anonymous connections refuses them at the point one is described rather
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
     * Names what Hibernate caches over a database, and opens it.
     *
     * <p>A builder only exists where a database does, so the defaults are the ones a database wants:
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
         * Opens the database and holds it, so the rows of every type it maps can be read and
         * written.
         *
         * <p>The mapped types are the database's, not a session's. A session registers the types it
         * holds a generation of, and a mapped type left out of that list is reached through the
         * returned source's Hibernate access instead.
         *
         * @param models the types the database maps
         * @param gson the parser custom Hibernate types bind through
         * @param logLevel the level the connection logs at
         * @return the open database, which the caller owns and must close once every session reading
         *         it is shut down
         */
        public @NotNull RelationalSource open(
            @NotNull ConcurrentList<Class<JpaModel>> models,
            @NotNull Gson gson,
            @NotNull Logging.Level logLevel
        ) {
            return new RelationalSource(this, models, gson, logLevel);
        }

    }

}

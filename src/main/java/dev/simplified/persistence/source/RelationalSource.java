package dev.simplified.persistence.source;

import com.google.gson.Gson;
import dev.simplified.annotations.BuilderIgnore;
import dev.simplified.annotations.BuilderNames;
import dev.simplified.annotations.ClassBuilder;
import dev.simplified.annotations.Cleanup;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.SetterNames;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.driver.JpaDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.type.TypeRegistrar;
import dev.simplified.reflection.Reflection;
import dev.simplified.util.Logging;
import jakarta.persistence.criteria.CriteriaQuery;
import org.ehcache.core.Ehcache;
import org.ehcache.jsr107.EhcacheCachingProvider;
import org.hibernate.Hibernate;
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
import javax.cache.spi.CachingProvider;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An open database, and the rows every type it maps are read and written through.
 *
 * <p>A database is named through the static of the {@link JpaDriver} that speaks to it, which fills in
 * its {@link Connection} - the driver, the url, and for a database that refuses anonymous connections
 * the account its static takes - and answers the {@link Builder} that opens it. A caller never renders
 * a url: the shape of the address is decided by which driver was reached for. {@link #builder()}
 * starts from the connection itself, for a driver with no static of its own.
 *
 * <p>The builder's defaults are the ones a database wants, each held as its field's initializer: both
 * caches on, read-write concurrency, a missing region created with a warning, a thirty-second query
 * result life, a sixty-second entity region life, the default {@link GsonSettings} parser and logging
 * at {@link Logging.Level#WARN}.
 *
 * <p>Everything Hibernate needs to exist is here and nowhere else - the service registry, the
 * metadata, the session factory and the cache manager its JCache regions live in, which no other
 * database shares - so a session reading a document source holds none of it. It is a
 * {@link Source.Writable} like any other source, which is what lets a repository read a table the
 * same way it reads a document.
 *
 * <p>Whoever opens one holds it, for the Hibernate access below and for the session it is handed to.
 * Its Hibernate access - {@link #with}, {@link #transaction} and the session factory a Hibernate
 * session hands out - reaches no {@link JpaSession}. A registered type written through it serves its
 * previous rows until its session rebuilds it; it is the way to read and write a type the session
 * leaves out of its models.
 *
 * <p>Closing it is optional: a JVM shutdown hook closes a database still open at exit, and
 * {@link #close()} releases it earlier. Every session reading it is shut down first, because a session
 * reading a closed database fails its next write, rebuild or tick. JVM shutdown hooks run
 * concurrently, so a rebuild or tick still running at exit can fail against a database that is
 * closing. An open that fails part way releases what it had acquired before it throws, and registers
 * no hook.
 *
 * <p>A relational source always accepts writes. Refusing one is the database's job, through the
 * permissions the connection was opened under, rather than this library's.
 *
 * @see JpaDriver
 * @see Connection
 */
@ClassBuilder(
    setters = @SetterNames(set = "with{}"),
    builder = @BuilderNames(builder = BuilderNames.NONE, from = BuilderNames.NONE, toBuilder = BuilderNames.NONE)
)
public final class RelationalSource implements Source.Writable, AutoCloseable {

    private static final @NotNull String TIMESTAMPS_REGION = "default-update-timestamps-region";
    private static final @NotNull String QUERY_RESULTS_REGION = "default-query-results-region";

    /**
     * Where it connects: the driver that speaks to it, the url reaching it and the account it is
     * reached with.
     */
    private final @NotNull Connection connection;

    /**
     * The types it maps, which are the database's rather than a session's: a session registers the
     * types it holds a generation of, and a mapped type left out of that list is reached through
     * {@link #with} and {@link #transaction} instead.
     */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /**
     * The parser custom Hibernate types bind through.
     */
    private final @NotNull Gson gson = GsonSettings.defaults().create();

    /**
     * The level every logger this database brings with it logs at.
     */
    private final @NotNull Logging.Level logLevel = Logging.Level.WARN;

    /**
     * Whether the Hibernate query cache is enabled.
     */
    private final boolean usingQueryCache = true;

    /**
     * Whether the Hibernate second-level cache is enabled.
     */
    private final boolean using2ndLevelCache = true;

    /**
     * Whether Hibernate gathers statistics.
     */
    private final boolean usingStatistics;

    /**
     * The concurrency strategy entities are cached under.
     */
    private final @NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy = CacheConcurrencyStrategy.READ_WRITE;

    /**
     * What becomes of a cache region Hibernate asks for and finds missing.
     */
    private final @NotNull CacheMissingStrategy cacheMissingStrategy = CacheMissingStrategy.CREATE_WARN;

    /**
     * How long a cached query result lives, in seconds, or {@code 0} for no expiry.
     */
    private final long queryResultsTTL = 30;

    /**
     * How long an entry of every mapped type's region lives after it is written, in milliseconds, or
     * {@code 0} for no expiry.
     */
    private final long cacheExpiryMs = 60_000;

    /**
     * The cache manager this database's regions live in, which no other database shares.
     */
    @BuilderIgnore
    private final @NotNull CacheManager cacheManager;

    /**
     * Hibernate entity metadata including custom type registrations and column adjustments.
     */
    @BuilderIgnore
    @Getter private final @NotNull Metadata metadata;

    /**
     * The Hibernate service registry backing {@link #sessionFactory}.
     */
    @BuilderIgnore
    private final @NotNull StandardServiceRegistry serviceRegistry;

    /**
     * The Hibernate session factory opened from {@link #metadata}.
     */
    @BuilderIgnore
    private final @NotNull SessionFactory sessionFactory;

    /**
     * JVM shutdown hook that closes this database at exit, removed by {@link #close()}.
     */
    @BuilderIgnore
    private final @NotNull Thread shutdownHook;

    /**
     * Opens the database a connection names, mapping the given types.
     *
     * @param connection where it connects
     * @param models the types it maps
     * @param gson the parser custom Hibernate types bind through
     * @param logLevel the level every logger it brings with it logs at
     * @param usingQueryCache whether the Hibernate query cache is enabled
     * @param using2ndLevelCache whether the Hibernate second-level cache is enabled
     * @param usingStatistics whether Hibernate gathers statistics
     * @param cacheConcurrencyStrategy the concurrency strategy entities are cached under
     * @param cacheMissingStrategy what becomes of a cache region Hibernate finds missing
     * @param queryResultsTTL how long a cached query result lives, in seconds, or {@code 0} for no
     *        expiry
     * @param cacheExpiryMs how long a region entry lives after it is written, in milliseconds, or
     *        {@code 0} for no expiry
     * @throws JpaException if no connection or no models are given, or the JDBC driver is not on the
     *         classpath
     */
    private RelationalSource(
        @Nullable Connection connection,
        @Nullable ConcurrentList<Class<JpaModel>> models,
        @NotNull Gson gson,
        @NotNull Logging.Level logLevel,
        boolean usingQueryCache,
        boolean using2ndLevelCache,
        boolean usingStatistics,
        @NotNull CacheConcurrencyStrategy cacheConcurrencyStrategy,
        @NotNull CacheMissingStrategy cacheMissingStrategy,
        long queryResultsTTL,
        long cacheExpiryMs
    ) {
        if (connection == null)
            throw new JpaException("A relational source names no connection");

        if (models == null)
            throw new JpaException("A relational source names no models to map");

        this.connection = connection;
        this.models = models;
        this.gson = gson;
        this.logLevel = logLevel;
        this.usingQueryCache = usingQueryCache;
        this.using2ndLevelCache = using2ndLevelCache;
        this.usingStatistics = usingStatistics;
        this.cacheConcurrencyStrategy = cacheConcurrencyStrategy;
        this.cacheMissingStrategy = cacheMissingStrategy;
        this.queryResultsTTL = queryResultsTTL;
        this.cacheExpiryMs = cacheExpiryMs;

        this.applyLogLevel(logLevel);
        this.requireDriverOnClasspath();

        // Everything acquired from here on is released again when opening fails part way, because
        // the caller receives no source to close. The cache manager is not asked for until the
        // driver is known to load, since the provider keeps every manager it hands out until that
        // manager is closed; asking under a class loader of the database's own is what keeps the
        // provider from answering one another database already holds.
        StandardServiceRegistry registry = null;
        SessionFactory factory = null;
        CachingProvider cachingProvider = Caching.getCachingProvider(EhcacheCachingProvider.class.getName());
        this.cacheManager = cachingProvider.getCacheManager(
            cachingProvider.getDefaultURI(),
            new URLClassLoader(new URL[0], cachingProvider.getDefaultClassLoader())
        );

        try {
            if (this.usingQueryCache) {
                this.buildCacheConfiguration(TIMESTAMPS_REGION, Duration.ETERNAL);
                this.buildCacheConfiguration(
                    QUERY_RESULTS_REGION,
                    this.queryResultsTTL <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.SECONDS, this.queryResultsTTL)
                );
            }

            registry = new StandardServiceRegistryBuilder()
                .applySettings(this.createProperties(logLevel))
                .build();
            this.serviceRegistry = registry;
            this.metadata = this.createMetadata(this.createMetadataSources().getMetadataBuilder(), gson);
            factory = this.metadata.buildSessionFactory();
            this.sessionFactory = factory;

            // Registered last, so an open that fails registers nothing; the JVM refuses it only
            // once it is already exiting.
            this.shutdownHook = new Thread(this::close, "relational-source-close");
            Runtime.getRuntime().addShutdownHook(this.shutdownHook);
        } catch (RuntimeException exception) {
            if (factory != null)
                factory.close();

            if (registry != null)
                StandardServiceRegistryBuilder.destroy(registry);

            if (!this.cacheManager.isClosed())
                this.cacheManager.close();

            throw exception;
        }
    }

    /**
     * Starts describing a database with where it connects: the driver, the url and the account.
     *
     * <p>A driver's own static fills the connection in and answers the {@link Builder} directly; this is
     * the start for a {@link JpaDriver} that names no database of its own.
     *
     * @return the builder taking the connection, whose {@code build()} leads into the {@link Builder}
     */
    public static @NotNull Connection.Builder builder() {
        return new Connection.Builder(new Builder());
    }

    /** {@inheritDoc} */
    @Override
    public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException {
        return this.with(hibernate -> {
            CriteriaQuery<T> query = hibernate.getCriteriaBuilder().createQuery(type);
            query.select(query.from(type));

            // A row the session already holds a proxy for - one a lazy association read earlier in
            // the same query named - comes back as that proxy, whose own fields are empty. The query
            // has initialized it, so reaching the entity behind it loads nothing.
            return hibernate.createQuery(query)
                .getResultList()
                .stream()
                .map(row -> Hibernate.unproxy(row, type))
                .collect(Concurrent.toUnmodifiableList());
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
     * Opens a managed session, passes it to the consumer, and closes it.
     *
     * @param consumer the operation to perform with the session
     * @throws JpaException if the session cannot be opened or the operation fails
     */
    public void with(@NotNull Consumer<Session> consumer) {
        try {
            @Cleanup Session session = this.sessionFactory.openSession();
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
            @Cleanup Session session = this.sessionFactory.openSession();
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
     * destroys the service registry and closes this database's cache manager, which no other
     * database shares.
     *
     * <p>Closing is optional: a JVM shutdown hook closes a database still open at exit. Closing it
     * explicitly releases it earlier and removes the hook, so the JVM no longer holds it; a call made
     * while the JVM is already exiting leaves the hook to the JVM. Every session reading this database
     * is shut down first, because a session reading a closed database fails its next write, rebuild or
     * tick.
     *
     * <p>A second call, from this thread or another, finds every step already done and does nothing
     * further.
     */
    @Override
    public synchronized void close() {
        try {
            Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
        } catch (IllegalStateException ignore) { }

        // Each step is a no-op once done: the factory ignores a second close, the registry a second
        // destroy, and the manager is asked whether it is closed.
        this.sessionFactory.close();
        StandardServiceRegistryBuilder.destroy(this.serviceRegistry);

        if (!this.cacheManager.isClosed())
            this.cacheManager.close();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String toString() {
        return String.format("%s (%s)", this.connection.getUrl(), this.connection.getDriver().getSchemaPolicy());
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
        Logging.setLevel(this.connection.getDriver().getClassPath(), level);
        Logging.setLevel(String.format("%s-%s", Ehcache.class, TIMESTAMPS_REGION), level);
        Logging.setLevel(String.format("%s-%s", Ehcache.class, QUERY_RESULTS_REGION), level);
        this.models.forEach(model -> Logging.setLevel(String.format("%s-%s", Ehcache.class, model.getName()), level));

        if (this.connection.getCredentials().isPresent())
            Logging.setLevel("com.zaxxer.hikari", level);
    }

    /**
     * Fails before Hibernate does when the JDBC driver this speaks through is not on the classpath.
     *
     * @throws JpaException if the driver class cannot be loaded
     */
    private void requireDriverOnClasspath() {
        String classPath = this.connection.getDriver().getClassPath();

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
     * @return the properties the service registry is built from
     */
    private @NotNull ConcurrentMap<String, Object> createProperties(@NotNull Logging.Level logLevel) {
        ConcurrentMap<String, Object> properties = Concurrent.newMap();

        this.connection.getCredentials().ifPresent(credentials -> {
            properties.put("hibernate.connection.username", credentials.getUser());
            properties.put("hibernate.connection.password", credentials.getPassword());
            properties.put("hibernate.connection.provider_class", "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");
            properties.put("hikari.maximumPoolSize", 20);
        });

        JpaDriver driver = this.connection.getDriver();
        String hbm2ddl = driver.getSchemaPolicy().getHbm2ddl();

        if (hbm2ddl != null)
            properties.put("hibernate.hbm2ddl.auto", hbm2ddl);

        properties.put("hibernate.dialect", driver.getDialectClass());
        properties.put("hibernate.connection.driver_class", driver.getClassPath());
        properties.put("hibernate.connection.url", this.connection.getUrl());
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
        properties.put("hibernate.cache.use_query_cache", this.usingQueryCache);
        properties.put("hibernate.cache.use_second_level_cache", this.using2ndLevelCache);
        properties.put("hibernate.javax.cache.missing_cache_strategy", this.cacheMissingStrategy.getExternalRepresentation());

        // Handing Hibernate's JCacheRegionFactory the manager itself stops it resolving one of its
        // own, which would be the provider's default and shared with every other database. The
        // hibernate.javax.cache prefix is the one hibernate-jcache reads; a jakarta-prefixed name is
        // not honored.
        properties.put("hibernate.javax.cache.cache_manager", this.cacheManager);

        if (this.cacheConcurrencyStrategy != CacheConcurrencyStrategy.NONE)
            properties.put("hibernate.cache.default_cache_concurrency_strategy", this.cacheConcurrencyStrategy.toAccessType().getExternalName());

        return properties.toUnmodifiable();
    }

    /**
     * Registers the mapped entity classes, each with a cache region of its own, and every region
     * with the one TTL this database was opened with.
     *
     * @return the metadata sources naming every mapped class
     */
    private @NotNull MetadataSources createMetadataSources() {
        MetadataSources metadataSources = new MetadataSources(this.serviceRegistry);
        Duration life = this.cacheExpiryMs <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.MILLISECONDS, this.cacheExpiryMs);

        for (Class<JpaModel> model : this.models) {
            this.buildCacheConfiguration(model.getName(), life);
            metadataSources.addAnnotatedClass(model);
        }

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

        if (this.connection.getDriver().getSchemaPolicy().isGenerated())
            adjustColumnLength(metadata);

        return metadata;
    }

    /**
     * Creates a JCache region in this database's cache manager with the given name and TTL.
     *
     * @param cacheName the region name
     * @param duration how long an entry lives after it is written
     */
    private void buildCacheConfiguration(@NotNull String cacheName, @NotNull Duration duration) {
        this.cacheManager.createCache(
            cacheName,
            new MutableConfiguration<>()
                .setStoreByValue(false)
                .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(duration))
        );
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
     * Collects what a database is opened with - its connection, the types it maps, the parser custom
     * Hibernate types bind through, its log level and what Hibernate caches over it - and opens it,
     * so the rows of every type it maps can be read and written.
     *
     * <p>Only a {@link Connection.Builder} leads here, so a builder holds its connection before a
     * caller sees it. The database it opens is the caller's to hold; the JVM closes it at exit if it
     * is still open, and a caller closing it earlier shuts every session reading it down first.
     */
    public static final class Builder {

        /**
         * Sets where the database connects.
         *
         * @param connection the driver, url and account
         * @return this builder
         */
        @NotNull Builder withConnection(@NotNull Connection connection) {
            this.connection = connection;
            return this;
        }

    }

}

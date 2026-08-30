package dev.simplified.persistence.store;

import com.google.gson.Gson;
import dev.simplified.annotations.Cleanup;
import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.JpaModel;
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

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An open database, and the rows every type it holds are read and written through.
 *
 * <p>Everything Hibernate needs to exist is here and nowhere else - the service registry, the
 * metadata, the session factory and the JCache regions - so a session that opened no database holds
 * none of it rather than holding four nulls. It is a {@link Source.Writable} like any other origin,
 * which is what lets a repository read a table the same way it reads a document.
 *
 * <p>A relational origin always accepts writes. Refusing one is the database's job, through the
 * permissions the connection was opened under, rather than this library's.
 *
 * @see RelationalOrigin#open
 */
@Getter
public final class Relational implements Source.Writable, AutoCloseable {

    /**
     * JCache TTL is set to this multiple of the refresh interval as a safety net. Under normal
     * operation the scheduler refreshes proactively; the JCache TTL only fires if the scheduler
     * misses multiple cycles.
     */
    private static final int CACHE_TTL_MULTIPLIER = 2;

    private static final @NotNull String TIMESTAMPS_REGION = "default-update-timestamps-region";
    private static final @NotNull String QUERY_RESULTS_REGION = "default-query-results-region";

    /**
     * The database this was opened against.
     */
    private final @NotNull RelationalOrigin origin;

    /**
     * The types registered against it.
     */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /**
     * Assembled Hibernate and HikariCP connection properties.
     */
    private final @NotNull ConcurrentMap<String, Object> properties;

    /**
     * Hibernate entity metadata including custom type registrations and column adjustments.
     */
    private final @NotNull Metadata metadata;

    /**
     * The Hibernate service registry backing {@link #sessionFactory}.
     */
    private final @NotNull StandardServiceRegistry serviceRegistry;

    /**
     * The Hibernate session factory opened from {@link #metadata}.
     */
    private final @NotNull SessionFactory sessionFactory;

    Relational(
        @NotNull RelationalOrigin origin,
        @NotNull ConcurrentList<Class<JpaModel>> models,
        @NotNull Gson gson,
        @NotNull Logging.Level logLevel
    ) {
        this.origin = origin;
        this.models = models;

        this.requireDriverOnClasspath();

        // The query-results and update-timestamps regions are only created when query caching is
        // actually enabled - a HAZELCAST_* provider disables it unconditionally, so creating these
        // would leave empty never-used JCache caches sitting on the cluster.
        if (this.isQueryCacheEnabled()) {
            this.buildCacheConfiguration(TIMESTAMPS_REGION, Duration.ETERNAL);
            long ttl = origin.getQueryResultsTTL();
            this.buildCacheConfiguration(
                QUERY_RESULTS_REGION,
                ttl <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.SECONDS, ttl)
            );
        }

        this.properties = this.createProperties(logLevel);
        this.serviceRegistry = new StandardServiceRegistryBuilder()
            .applySettings(this.properties)
            .build();
        this.metadata = this.createMetadata(this.createMetadataSources(logLevel).getMetadataBuilder(), gson);
        this.sessionFactory = this.metadata.buildSessionFactory();
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
     * Whether Hibernate's query cache is active for this database.
     *
     * <p>A Hazelcast provider disables it unconditionally: the query results region wraps results in
     * a {@code Serializable} holder that Hazelcast routes through {@code ObjectOutputStream}, which
     * walks the object graph with no hook for its own serialization service. Nothing on the read path
     * consults the region, so disabling it removes the last code path that touches it.
     */
    private boolean isQueryCacheEnabled() {
        JpaCacheProvider provider = this.origin.getCacheProvider();

        return this.origin.isUsingQueryCache()
            && provider != JpaCacheProvider.HAZELCAST_CLIENT
            && provider != JpaCacheProvider.HAZELCAST_EMBEDDED;
    }

    /**
     * Fails before Hibernate does when the JDBC driver this speaks through is not on the classpath.
     *
     * @throws JpaException if the driver class cannot be loaded
     */
    private void requireDriverOnClasspath() {
        String classPath = this.origin.getDriver().getClassPath();

        try {
            Class.forName(classPath);
        } catch (ClassNotFoundException exception) {
            throw new JpaException(exception, "No JDBC driver '%s' on the classpath for '%s'", classPath, this.origin);
        }
    }

    /**
     * Assembles Hibernate and HikariCP properties from the origin.
     *
     * @param logLevel the level the connection logs at
     */
    private @NotNull ConcurrentMap<String, Object> createProperties(@NotNull Logging.Level logLevel) {
        ConcurrentMap<String, Object> properties = Concurrent.newMap();

        this.origin.getCredentials().ifPresentOrElse(
            credentials -> {
                properties.put("hibernate.connection.username", credentials.user());
                properties.put("hibernate.connection.password", credentials.password());
                properties.put("hibernate.connection.provider_class", "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");
                properties.put("hikari.maximumPoolSize", 20);
            },
            () -> {}
        );

        String hbm2ddl = this.origin.getSchemaPolicy().getHbm2ddl();

        if (hbm2ddl != null)
            properties.put("hibernate.hbm2ddl.auto", hbm2ddl);

        properties.put("hibernate.dialect", this.origin.getDriver().getDialectClass());
        properties.put("hibernate.connection.driver_class", this.origin.getDriver().getClassPath());
        properties.put("hibernate.connection.url", this.origin.getUrl());
        properties.put("hibernate.globally_quoted_identifiers", true);

        properties.put("hibernate.jdbc.log.warnings", logLevel.includes(Logging.Level.WARN));
        properties.put("hibernate.show_sql", logLevel.includes(Logging.Level.DEBUG));
        properties.put("hibernate.format_sql", logLevel.includes(Logging.Level.TRACE));
        properties.put("hibernate.highlight_sql", logLevel.includes(Logging.Level.TRACE));
        properties.put("hibernate.use_sql_comments", logLevel.includes(Logging.Level.DEBUG));

        properties.put("hibernate.generate_statistics", this.origin.isUsingStatistics());

        properties.put("hibernate.order_inserts", true);
        properties.put("hibernate.order_updates", true);

        properties.put("hibernate.jdbc.batch_size", 100);
        properties.put("hibernate.jdbc.fetch_size", 400);
        properties.put("hibernate.jdbc.use_get_generated_keys", true);

        properties.put("hibernate.cache.region.factory_class", "jcache");
        properties.put("hibernate.cache.use_reference_entries", true);
        properties.put("hibernate.cache.use_structured_entries", logLevel.includes(Logging.Level.DEBUG));
        properties.put("hibernate.cache.use_query_cache", this.isQueryCacheEnabled());
        properties.put("hibernate.cache.use_second_level_cache", this.origin.isUsing2ndLevelCache());
        properties.put("hibernate.javax.cache.missing_cache_strategy", this.origin.getMissingCacheStrategy().getExternalRepresentation());

        // Pin the JCache provider for Hibernate's internal JCacheRegionFactory so it does not call
        // the no-arg lookup, which throws when more than one provider sits on the runtime classpath.
        // Property names use the hibernate.javax.cache.* prefix per hibernate-jcache 7.3 - the
        // jakarta-prefixed equivalents are not honored.
        JpaCacheProvider cacheProvider = this.origin.getCacheProvider();
        properties.put("hibernate.javax.cache.provider", cacheProvider.getProviderClassName());

        if (cacheProvider.getConfigUri() != null)
            properties.put("hibernate.javax.cache.uri", cacheProvider.getConfigUri());

        if (this.origin.getCacheConcurrencyStrategy() != CacheConcurrencyStrategy.NONE)
            properties.put("hibernate.cache.default_cache_concurrency_strategy", this.origin.getCacheConcurrencyStrategy().toAccessType().getExternalName());

        return properties.toUnmodifiable();
    }

    /**
     * Registers annotated entity classes and configures per-entity cache logging.
     */
    private @NotNull MetadataSources createMetadataSources(@NotNull Logging.Level logLevel) {
        MetadataSources metadataSources = new MetadataSources(this.serviceRegistry);

        this.models
            .stream()
            .map(this::buildCacheConfiguration)
            .peek(metadataSources::addAnnotatedClass)
            .forEach(model -> Logging.setLevel(String.format("%s-%s", Ehcache.class, model.getName()), logLevel));

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

        if (this.origin.getSchemaPolicy().isGenerated())
            adjustColumnLength(metadata);

        return metadata;
    }

    /**
     * Creates a JCache configuration for one type, with the TTL its {@link Hydration} cadence or the
     * origin's default asks for, multiplied as a safety net.
     */
    private @NotNull Class<JpaModel> buildCacheConfiguration(@NotNull Class<JpaModel> type) {
        Hydration hydration = type.getAnnotation(Hydration.class);
        long expiryMs = hydration != null && hydration.every() > 0
            ? hydration.unit().toMillis(hydration.every())
            : this.origin.getDefaultCacheExpiryMs();

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
     * Resolves the JCache manager for the origin's provider, opening it against the provider's
     * configuration resource when it names one.
     *
     * @return the cache manager for the configured provider
     */
    private @NotNull CacheManager resolveCacheManager() {
        JpaCacheProvider provider = this.origin.getCacheProvider();
        javax.cache.spi.CachingProvider cachingProvider = Caching.getCachingProvider(provider.getProviderClassName());

        if (provider.getConfigUri() == null)
            return cachingProvider.getCacheManager();

        try {
            return cachingProvider.getCacheManager(new URI(provider.getConfigUri()), cachingProvider.getDefaultClassLoader());
        } catch (URISyntaxException exception) {
            throw new JpaException(exception, "Invalid cache provider config URI '%s'", provider.getConfigUri());
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

}

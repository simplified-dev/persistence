package dev.simplified.persistence;

import com.google.gson.Gson;
import dev.simplified.annotations.Cleanup;
import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.tuple.single.LifecycleSingleStream;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.store.Source;
import dev.simplified.persistence.store.WriteRequest;
import dev.simplified.persistence.type.TypeRegistrar;
import jakarta.persistence.criteria.CriteriaQuery;
import dev.simplified.reflection.Reflection;
import dev.simplified.scheduler.Scheduler;
import dev.simplified.util.Logging;
import dev.simplified.util.time.Stopwatch;
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
import org.hibernate.tool.schema.SourceType;
import org.hibernate.tool.schema.TargetType;
import org.hibernate.tool.schema.internal.ExceptionHandlerHaltImpl;
import org.hibernate.tool.schema.internal.exec.ScriptTargetOutputToFile;
import org.hibernate.tool.schema.spi.ContributableMatcher;
import org.hibernate.tool.schema.spi.ExceptionHandler;
import org.hibernate.tool.schema.spi.ExecutionOptions;
import org.hibernate.tool.schema.spi.SchemaManagementTool;
import org.hibernate.tool.schema.spi.ScriptSourceInput;
import org.hibernate.tool.schema.spi.ScriptTargetOutput;
import org.hibernate.tool.schema.spi.SourceDescriptor;
import org.hibernate.tool.schema.spi.TargetDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A fully self-initializing JPA session backed by a Hibernate {@link SessionFactory}
 * with JCache (EhCache) second-level caching.
 *
 * <p>The constructor performs the complete Hibernate bootstrap - building the
 * {@link StandardServiceRegistry}, discovering {@link TypeRegistrar} implementations,
 * constructing {@link Metadata}, and opening the {@link SessionFactory}. Once constructed,
 * the session is immediately active and ready for use.</p>
 *
 * <p>Typical lifecycle managed by {@link SessionManager}:</p>
 * <ol>
 *     <li><b>Construction</b> - builds JCache regions with per-entity TTL, service registry,
 *         metadata, and session factory</li>
 *     <li>{@link #cacheRepositories()} - creates a {@link JpaRepository} per discovered model
 *         and performs the initial data load</li>
 *     <li>{@link #shutdown()} - clears repositories, shuts down the scheduler, closes the
 *         session factory, destroys the service registry, and removes JCache regions</li>
 * </ol>
 *
 * <p>Provides repository lookup via {@link #getRepository(Class)}, managed session access via
 * {@link #with(Consumer)} / {@link #with(Function)}, and transactional execution via
 * {@link #transaction(Consumer)} / {@link #transaction(Function)}.</p>
 *
 * @see JpaConfig
 * @see JpaRepository
 * @see SessionManager
 */
@Getter
public final class JpaSession {

    /**
     * JCache TTL is set to this multiple of the refresh interval as a safety net.
     * Under normal operation the scheduler refreshes proactively; the JCache TTL
     * only fires if the scheduler misses multiple cycles.
     */
    private static final int CACHE_TTL_MULTIPLIER = 2;

    /**
     * Cached repositories keyed by their entity class.
     */
    private final @NotNull ConcurrentMap<Class<? extends JpaModel>, Repository<? extends JpaModel>> repositories = Concurrent.newMap();

    /**
     * Topologically sorted entity classes discovered from the {@link RepositoryFactory}.
     */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /**
     * The immutable configuration that produced this session.
     */
    private final @NotNull JpaConfig config;

    /**
     * Internal scheduler for repository refresh tasks, shut down on {@link #shutdown()}.
     */
    private final @NotNull Scheduler scheduler;

    /**
     * Gson instance configured with this session's {@link GsonSettings}.
     */
    private final @NotNull Gson gson;

    /**
     * Assembled Hibernate and HikariCP connection properties.
     */
    private final @NotNull ConcurrentMap<String, Object> properties;

    /**
     * Hibernate entity metadata including custom type registrations and column adjustments.
     */
    private final @NotNull Metadata metadata;

    /**
     * The Hibernate session factory opened from {@link #metadata}.
     */
    private final @NotNull SessionFactory sessionFactory;

    /**
     * The Hibernate service registry backing {@link #sessionFactory}.
     */
    private final @NotNull StandardServiceRegistry serviceRegistry;

    /**
     * Timing snapshot of the full constructor bootstrap.
     */
    private final @NotNull Stopwatch initialization;

    /**
     * {@code true} while this session has not been shut down.
     */
    private boolean active = true;

    /**
     * Guard flag preventing {@link #cacheRepositories()} from running more than once.
     */
    private boolean repositoriesCached = false;

    /**
     * Timing snapshot of the {@link #cacheRepositories()} pass, or {@code null} if not yet run.
     */
    private Stopwatch repositoryCache;

    /**
     * Constructs a fully initialized session from the given configuration.
     *
     * <p>Resolves the model list via {@link RepositoryFactory#getModels()}, builds JCache regions
     * for query and timestamp caching, assembles Hibernate properties, creates the
     * {@link StandardServiceRegistry}, discovers and runs {@link TypeRegistrar} implementations,
     * builds {@link Metadata} (with column length adjustments for embedded drivers), and opens
     * the {@link SessionFactory}.</p>
     *
     * @param config the configuration defining driver, repository factory, and connection settings
     */
    public JpaSession(@NotNull JpaConfig config) {
        Instant startTime = Instant.now();
        this.models = config.getRepositoryFactory().getModels();
        this.config = config;
        this.scheduler = new Scheduler();
        this.gson = config.getGsonSettings().create();

        // Build JCache regions. The query-results and update-timestamps regions are only
        // created when query caching is actually enabled - for HAZELCAST_* providers Phase 2d
        // unconditionally disables query caching, so creating these regions would leave empty
        // never-used JCache caches sitting on the cluster.
        boolean queryCacheActive = config.isUsingQueryCache()
            && config.getCacheProvider() != JpaCacheProvider.HAZELCAST_CLIENT
            && config.getCacheProvider() != JpaCacheProvider.HAZELCAST_EMBEDDED;
        if (queryCacheActive) {
            this.buildCacheConfiguration("default-update-timestamps-region", Duration.ETERNAL);
            long ttl = config.getQueryResultsTTL();
            Duration queryDuration = ttl <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.SECONDS, ttl);
            this.buildCacheConfiguration("default-query-results-region", queryDuration);
        }

        // Build Hibernate infrastructure
        this.properties = this.createProperties();
        this.serviceRegistry = new StandardServiceRegistryBuilder()
            .applySettings(this.properties)
            .build();
        MetadataSources sources = this.createMetadataSources(this.serviceRegistry);
        this.metadata = this.createMetadata(sources.getMetadataBuilder());
        this.sessionFactory = this.metadata.buildSessionFactory();

        this.initialization = Stopwatch.of(startTime);
    }

    /**
     * Creates a JCache configuration for the given entity type with TTL from {@link CacheExpiry}
     * or the config default, multiplied by {@link #CACHE_TTL_MULTIPLIER} as a safety net.
     */
    private @NotNull Class<JpaModel> buildCacheConfiguration(@NotNull Class<JpaModel> type) {
        CacheExpiry cacheExpiry = type.getAnnotation(CacheExpiry.class);
        long expiryMs = cacheExpiry != null
            ? cacheExpiry.length().toMillis(cacheExpiry.value())
            : this.config.getDefaultCacheExpiryMs();

        long jcacheTtlMs = expiryMs <= 0 ? 0 : expiryMs * CACHE_TTL_MULTIPLIER;
        Duration duration = jcacheTtlMs <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.MILLISECONDS, jcacheTtlMs);
        this.buildCacheConfiguration(type.getName(), duration);
        return type;
    }

    /**
     * Creates a JCache configuration with the given name and TTL, reusing existing caches.
     */
    private void buildCacheConfiguration(@NotNull String cacheName, @NotNull Duration duration) {
        CacheManager cacheManager = this.resolveCacheManager();

        if (cacheManager.getCache(cacheName, Object.class, Object.class) != null)
            return;

        MutableConfiguration<Object, Object> cacheConfiguration = new MutableConfiguration<>()
            .setStoreByValue(false)
            .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(duration));

        cacheManager.createCache(cacheName, cacheConfiguration);
    }

    /**
     * Resolves the JCache {@link CacheManager} for the {@link JpaCacheProvider} configured
     * on {@link #config}.
     *
     * <p>Looks up the provider by its fully-qualified class name and, when the provider
     * declares a non-null {@link JpaCacheProvider#getConfigUri() configUri}, opens the
     * cache manager against that classpath resource URI. Otherwise the provider's default
     * cache manager is returned.</p>
     *
     * @return the cache manager for the configured provider
     * @throws javax.cache.CacheException if the provider class is not on the classpath
     */
    private @NotNull CacheManager resolveCacheManager() {
        JpaCacheProvider provider = this.config.getCacheProvider();
        javax.cache.spi.CachingProvider cachingProvider = Caching.getCachingProvider(provider.getProviderClassName());

        if (provider.getConfigUri() == null)
            return cachingProvider.getCacheManager();

        try {
            return cachingProvider.getCacheManager(
                new java.net.URI(provider.getConfigUri()),
                cachingProvider.getDefaultClassLoader()
            );
        } catch (java.net.URISyntaxException ex) {
            throw new JpaException(ex, "Invalid cache provider config URI '%s'", provider.getConfigUri());
        }
    }

    /**
     * Assembles Hibernate and HikariCP properties from the {@link JpaConfig}.
     */
    private @NotNull ConcurrentMap<String, Object> createProperties() {
        ConcurrentMap<String, Object> properties = Concurrent.newMap();

        if (this.config.getDriver().isEmbedded()) {
            // Embedded: create schema fresh on each startup; no connection pool needed
            properties.put("hibernate.hbm2ddl.auto", "create-drop");
        } else {
            // External RDBMS: full connection pool
            properties.put("hibernate.connection.username", this.config.getUser());
            properties.put("hibernate.connection.password", this.config.getPassword());
            properties.put("hibernate.connection.provider_class", "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");
            properties.put("hikari.maximumPoolSize", 20);
        }

        properties.put("hibernate.dialect", this.config.getDriver().getDialectClass());
        properties.put("hibernate.connection.driver_class", this.config.getDriver().getClassPath());
        properties.put("hibernate.globally_quoted_identifiers", true);

        properties.put("hibernate.connection.url", this.config.getDriver().getConnectionUrl(
            this.config.getHost(),
            this.config.getPort(),
            this.config.getSchema()
        ));

        properties.put("hibernate.jdbc.log.warnings", this.config.isLogLevel(Logging.Level.WARN));
        properties.put("hibernate.show_sql", this.config.isLogLevel(Logging.Level.DEBUG));
        properties.put("hibernate.format_sql", this.config.isLogLevel(Logging.Level.TRACE));
        properties.put("hibernate.highlight_sql", this.config.isLogLevel(Logging.Level.TRACE));
        properties.put("hibernate.use_sql_comments", this.config.isLogLevel(Logging.Level.DEBUG));

        properties.put("hibernate.generate_statistics", this.config.isUsingStatistics());

        properties.put("hibernate.order_inserts", true);
        properties.put("hibernate.order_updates", true);

        properties.put("hibernate.jdbc.batch_size", 100);
        properties.put("hibernate.jdbc.fetch_size", 400);
        properties.put("hibernate.jdbc.use_get_generated_keys", true);

        // Cache
        properties.put("hibernate.cache.region.factory_class", "jcache");
        properties.put("hibernate.cache.use_reference_entries", true);
        properties.put("hibernate.cache.use_structured_entries", this.config.isLogLevel(Logging.Level.DEBUG));
        // Phase 2d: query cache is unconditionally disabled for HAZELCAST_* providers because
        // the Hibernate query results region wraps results in QueryResultsCacheImpl$CacheItem
        // (Serializable), which Hazelcast routes through ObjectOutputStream - and that stream
        // walks the object graph via Java's default protocol with no hook for Hazelcast's
        // SerializationService. The lazy stream() rewrite (also Phase 2d) makes the query
        // results cache vestigial for the application path, so disabling it here removes the
        // last code path that touches it. EhCache callers are unaffected.
        JpaCacheProvider cacheProvider = this.config.getCacheProvider();
        boolean queryCacheEnabled = this.config.isUsingQueryCache()
            && cacheProvider != JpaCacheProvider.HAZELCAST_CLIENT
            && cacheProvider != JpaCacheProvider.HAZELCAST_EMBEDDED;
        properties.put("hibernate.cache.use_query_cache", queryCacheEnabled);
        properties.put("hibernate.cache.use_second_level_cache", this.config.isUsing2ndLevelCache());
        properties.put("hibernate.javax.cache.missing_cache_strategy", this.config.getMissingCacheStrategy().getExternalRepresentation());

        // Pin the JCache provider for Hibernate's internal JCacheRegionFactory so it does not
        // call the no-arg Caching.getCachingProvider() lookup, which throws when more than one
        // provider sits on the runtime classpath (e.g. EhCache + Hazelcast in the test suite).
        // Property names use the hibernate.javax.cache.* prefix per ConfigSettings.PROP_PREFIX
        // in hibernate-jcache 7.3 - the jakarta-prefixed equivalents are not honored.
        properties.put("hibernate.javax.cache.provider", cacheProvider.getProviderClassName());
        if (cacheProvider.getConfigUri() != null)
            properties.put("hibernate.javax.cache.uri", cacheProvider.getConfigUri());

        if (this.config.getCacheConcurrencyStrategy() != CacheConcurrencyStrategy.NONE)
            properties.put("hibernate.cache.default_cache_concurrency_strategy", this.config.getCacheConcurrencyStrategy().toAccessType().getExternalName());

        return properties.toUnmodifiable();
    }

    /**
     * Registers annotated entity classes and configures per-entity cache logging.
     */
    private @NotNull MetadataSources createMetadataSources(@NotNull StandardServiceRegistry serviceRegistry) {
        MetadataSources metadataSources = new MetadataSources(serviceRegistry);

        this.getModels()
            .stream()
            .map(this::buildCacheConfiguration)
            .peek(metadataSources::addAnnotatedClass)
            .forEach(modelType -> Logging.setLevel(
                String.format("%s-%s", Ehcache.class, modelType.getName()),
                this.config.getLogLevel()
            ));

        return metadataSources;
    }

    /**
     * Discovers {@link TypeRegistrar} implementations via classpath scanning, scans entity
     * fields for custom type annotations, registers the types with the {@link MetadataBuilder},
     * builds the {@link Metadata}, post-processes type bindings, and adjusts column lengths
     * for embedded drivers.
     *
     * @param metadataBuilder the builder to register custom types with
     * @return the fully built and post-processed metadata
     */
    private @NotNull Metadata createMetadata(@NotNull MetadataBuilder metadataBuilder) {
        ConcurrentList<TypeRegistrar> registrars = Reflection.getResources()
            .filterPackage(TypeRegistrar.class)
            .getSubtypesOf(TypeRegistrar.class)
            .stream()
            .filter(cls -> !cls.isInterface() && !Modifier.isAbstract(cls.getModifiers()))
            .map(cls -> (TypeRegistrar) new Reflection<>(cls).newInstance())
            .collect(Concurrent.toList());

        registrars.forEach(registrar -> {
            registrar.scan(this.getGson(), this.getModels());
            registrar.register(metadataBuilder);
        });

        Metadata metadata = metadataBuilder.build();
        registrars.forEach(registrar -> registrar.postProcess(metadata));
        this.adjustColumnLength(metadata);
        return metadata;
    }

    /**
     * Widens all default-length ({@link Column#getLength() 255}) {@code VARCHAR} columns
     * to 1,000,000 for embedded drivers, preventing truncation of JSON-sourced data whose
     * lengths are unpredictable.
     * <p>
     * This is a no-op for non-embedded drivers where column sizes are governed by the
     * production schema.
     *
     * @param metadata the built Hibernate metadata whose column definitions are adjusted in place
     */
    private void adjustColumnLength(@NotNull Metadata metadata) {
        if (!this.getConfig().getDriver().isEmbedded())
            return;

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
     * Creates a {@link JpaRepository} for each discovered model via the configured
     * {@link RepositoryFactory} and performs the initial data load.
     *
     * <p>This method may only be called once per session. Subsequent calls throw.</p>
     *
     * @throws JpaException if repositories have already been cached
     */
    public void cacheRepositories() {
        if (!this.isRepositoriesCached()) {
            this.repositoriesCached = true;
            Instant startTime = Instant.now();

            for (Class<JpaModel> model : this.models)
                this.repositories.put(model, this.createRepository(model));

            this.hydrate(this.models);
            this.repositoryCache = Stopwatch.of(startTime);

            // A type asks for a cadence through @Hydration; one that declares none is hydrated here
            // and left alone. The tick runs at the shortest declared interval and rebuilds only what
            // has come due.
            long minIntervalMs = this.repositories.values()
                .stream()
                .filter(JpaRepository.class::isInstance)
                .map(repository -> (JpaRepository<?>) repository)
                .mapToLong(repository -> repository.getHydrationInterval().toMillis())
                .filter(ms -> ms > 0)
                .min()
                .orElse(0);

            if (minIntervalMs > 0)
                this.scheduler.scheduleAsync(this::hydrateDue, minIntervalMs, minIntervalMs, TimeUnit.MILLISECONDS);
        } else
            throw new JpaException("Session has already cached repositories");
    }

    /**
     * Creates the repository for one entity type, reading its origin off the configured factory.
     *
     * @param type the entity class
     * @param <T> the entity type
     * @return the repository for that type
     */
    private <T extends JpaModel> @NotNull JpaRepository<T> createRepository(@NotNull Class<T> type) {
        Source declared = this.config.getRepositoryFactory().sourceFor(type);
        return new JpaRepository<>(this, type, declared == Source.none() ? this.relational() : declared);
    }

    /**
     * The source for types whose rows the database itself authors.
     *
     * <p>A factory that declares no origin for a type is saying the database is the origin, so this
     * stands in for {@link Source#none()} at construction. Substituting once, here, is what lets every
     * repository hold a generation and answer from an index without a read ever asking where its rows
     * came from.
     *
     * @return a source reading whole tables through Hibernate
     */
    private @NotNull Source relational() {
        return new Source.Writable() {

            @Override
            public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
                return JpaSession.this.with(hibernate -> {
                    CriteriaQuery<T> query = hibernate.getCriteriaBuilder().createQuery(type);
                    query.select(query.from(type));
                    return Concurrent.newUnmodifiableList(hibernate.createQuery(query).getResultList());
                });
            }

            /**
             * {@inheritDoc}
             *
             * <p>A relational origin always accepts writes. Refusing one is the database's job, through
             * the permissions the connection was opened under, rather than this library's.
             */
            @Override
            public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) {
                if (request.rows().isEmpty())
                    return;

                if (request.operation() == WriteRequest.Operation.DELETE) {
                    JpaSession.this.transaction((Consumer<Session>) hibernate -> request.rows().forEach(hibernate::remove));
                    return;
                }

                // upsertMultiple bypasses dirty checking entirely, which is what keeps a row carrying a
                // read-only join column from raising HHH000502 when its association is not loaded.
                try (StatelessSession stateless = JpaSession.this.sessionFactory.openStatelessSession()) {
                    stateless.getTransaction().begin();
                    stateless.upsertMultiple(request.rows());
                    stateless.getTransaction().commit();
                } catch (JpaException jpaException) {
                    throw jpaException;
                } catch (Exception exception) {
                    throw new JpaException(exception, "Failed to write '%s'", request.type().getName());
                }
            }

        };
    }

    /**
     * Reads every registered type, then resolves every link across them.
     *
     * <p>Two passes rather than one because a link reaches rows another repository holds, so every
     * repository has to have read before any of them can be linked. That is also what removes the
     * ordering constraint the model sort used to carry.
     *
     * <p>A failing type aborts the pass. Continuing would publish a generation whose links point into
     * a type that never read, which is a wrong answer rather than a missing one.
     *
     * @param models the types to rebuild
     */
    private void hydrate(@NotNull Iterable<Class<JpaModel>> models) {
        ConcurrentList<JpaRepository<?>> hydrated = Concurrent.newList();

        for (Class<JpaModel> model : models) {
            if (this.repositories.get(model) instanceof JpaRepository<?> repository) {
                repository.hydrate();
                hydrated.add(repository);
            }
        }

        hydrated.forEach(JpaRepository::link);
    }

    /**
     * Rebuilds every type whose {@link Hydration} cadence has come due, and marks the rest stale when
     * they have stood too long.
     */
    private void hydrateDue() {
        ConcurrentList<Class<JpaModel>> due = Concurrent.newList();

        for (Class<JpaModel> model : this.models) {
            if (!(this.repositories.get(model) instanceof JpaRepository<?> repository))
                continue;

            if (repository.isDue())
                due.add(model);
            else if (repository.isPastStaleness())
                repository.markStale();
        }

        if (due.notEmpty())
            this.hydrate(due);
    }

    /**
     * Applies one write to the origin that owns the type, then rebuilds that type.
     *
     * <p>The rebuild is what keeps a held generation honest after a write: the rows the origin now
     * holds are not the rows this session read. It is driven by the write rather than by a caller
     * asking for it, so nothing downstream gains a way to force a rehydration.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the session is inactive, the type is unregistered, or its source holds
     *         no write instruction
     */
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        if (!(this.getRepository(request.type()) instanceof JpaRepository<M> repository))
            throw new JpaException("Repository for '%s' cannot be written through", request.type().getName());

        if (!(repository.getSource() instanceof Source.Writable writable))
            throw new JpaException("Source for '%s' holds no write instruction", request.type().getName());

        writable.write(request);
        repository.hydrate();
        repository.link();
    }


    /**
     * Exports this session's DDL schema to a persistent H2 file database for IDE
     * JPA column resolution.
     *
     * <p>Uses Hibernate's {@link SchemaManagementTool} SPI to generate DDL from the
     * session's {@link Metadata}, then executes the DDL against a new H2 file database
     * at {@code outputDir/<schema-name>}.
     *
     * @param outputDir the directory to write the H2 database files into
     */
    public void exportSchema(@NotNull Path outputDir) {
        String baseName = this.config.getSchema();
        outputDir.toFile().mkdirs();

        Path sqlFile = outputDir.resolve(baseName + "-schema.sql");

        try {
            Files.deleteIfExists(sqlFile);
        } catch (IOException e) {
            throw new JpaException(e);
        }

        SchemaManagementTool tool = this.getServiceRegistry().getService(SchemaManagementTool.class);

        if (tool == null)
            return;

        ConcurrentMap<String, Object> configValues = Concurrent.newMap(this.properties);

        ExecutionOptions executionOptions = new ExecutionOptions() {
            @Override
            public Map<String, Object> getConfigurationValues() {
                return configValues;
            }

            @Override
            public boolean shouldManageNamespaces() {
                return false;
            }

            @Override
            public ExceptionHandler getExceptionHandler() {
                return ExceptionHandlerHaltImpl.INSTANCE;
            }
        };

        SourceDescriptor sourceDescriptor = new SourceDescriptor() {
            @Override
            public SourceType getSourceType() {
                return SourceType.METADATA;
            }

            @Override
            public @Nullable ScriptSourceInput getScriptSourceInput() {
                return null;
            }
        };

        ScriptTargetOutput scriptOutput = new ScriptTargetOutputToFile(sqlFile.toFile(), "UTF-8");
        TargetDescriptor targetDescriptor = new TargetDescriptor() {
            @Override
            public @NotNull EnumSet<TargetType> getTargetTypes() {
                return EnumSet.of(TargetType.SCRIPT);
            }

            @Override
            public ScriptTargetOutput getScriptTargetOutput() {
                return scriptOutput;
            }
        };

        tool.getSchemaCreator(configValues).doCreation(
            this.metadata,
            executionOptions,
            ContributableMatcher.ALL,
            sourceDescriptor,
            targetDescriptor
        );

        // Create persistent H2 file database from the DDL
        Path dbFile = outputDir.resolve(baseName);
        String jdbcUrl = "jdbc:h2:file:" + dbFile;

        try {
            Files.deleteIfExists(Path.of(dbFile + ".mv.db"));
            Files.deleteIfExists(Path.of(dbFile + ".trace.db"));
        } catch (IOException e) {
            throw new JpaException(e);
        }

        try (
            Connection conn = DriverManager.getConnection(jdbcUrl, "sa", "");
            Statement stmt = conn.createStatement()
        ) {
            String ddl = Files.readString(sqlFile);

            for (String sql : ddl.split(";")) {
                sql = sql.trim();

                if (!sql.isEmpty())
                    stmt.execute(sql);
            }
        } catch (SQLException | IOException e) {
            throw new JpaException(e);
        }

        // DDL file was only needed to seed the H2 file database
        try {
            Files.deleteIfExists(sqlFile);
        } catch (IOException e) {
            throw new JpaException(e);
        }

    }

    /**
     * Retrieves the {@link Repository} for the given model class, searching by exact key
     * match first, then by assignability.
     *
     * @param tClass the entity class to look up
     * @param <M> the entity type
     * @return the matching repository
     * @throws JpaException if the session is not active or no matching repository exists
     */
    @SuppressWarnings("unchecked")
    public <M extends JpaModel> @NotNull Repository<M> getRepository(@NotNull Class<M> tClass) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        if (this.repositories.containsKey(tClass))
            return (Repository<M>) this.repositories.get(tClass);

        for (Map.Entry<Class<? extends JpaModel>, Repository<? extends JpaModel>> entry : this.repositories) {
            if (tClass.isAssignableFrom(entry.getKey()))
                return (Repository<M>) entry.getValue();
        }

        throw new JpaException("Repository for " + tClass.getName() + " not found");
    }

    /**
     * Checks whether a {@link Repository} for the given type (or a subtype) is registered
     * in this session.
     *
     * @param tClass the model class to check
     * @return {@code true} if a matching repository exists, {@code false} if not active or not found
     */
    public boolean hasRepository(@NotNull Class<?> tClass) {
        if (!this.isActive())
            return false;

        if (this.repositories.containsKey(tClass))
            return true;

        for (Map.Entry<Class<? extends JpaModel>, Repository<? extends JpaModel>> entry : this.repositories) {
            if (tClass.isAssignableFrom(entry.getKey()))
                return true;
        }

        return false;
    }

    /**
     * Opens a new Hibernate {@link Session} from the underlying {@link SessionFactory}.
     *
     * @return a freshly opened session
     */
    public @NotNull Session openSession() {
        return this.sessionFactory.openSession();
    }

    /**
     * Opens a Hibernate {@link Session} that is NOT auto-closed by this class. Caller assumes
     * full ownership of the session lifecycle and MUST close it - typically by handing it to
     * a {@link LifecycleSingleStream} which auto-closes
     * inside its terminal operations.
     *
     * <p>Use this when the result of a session-bound call (e.g. a lazy {@code getResultStream()})
     * outlives the calling method - the standard {@link #with(java.util.function.Function)}
     * pattern would close the session before any terminal consumed the stream. For all other
     * use cases prefer {@link #with(java.util.function.Consumer)} or
     * {@link #with(java.util.function.Function)} which auto-close.</p>
     *
     * @return a freshly opened session whose close is the caller's responsibility
     * @see JpaRepository#stream(Session)
     */
    public @NotNull Session openScopedSession() {
        return this.sessionFactory.openSession();
    }

    /**
     * Opens a managed {@link Session}, executes the consumer within a transaction
     * (begin + commit), and auto-closes the session.
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
     * Opens a managed {@link Session}, executes the function within a transaction
     * (begin + commit), returns the result, and auto-closes the session.
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
     * Performs an orderly shutdown of this session.
     *
     * <p>Marks the session as inactive, clears all repositories, shuts down the internal
     * {@link Scheduler}, closes the {@link SessionFactory} (which drops the schema for
     * embedded drivers), destroys the {@link StandardServiceRegistry}, and removes all
     * JCache regions created during construction.</p>
     *
     * <p>After shutdown, {@link #getRepository(Class)} and {@link #hasRepository(Class)}
     * will reject or deny all lookups. The session object should be discarded.</p>
     */
    void shutdown() {
        this.active = false;
        this.repositories.clear();
        this.scheduler.shutdown();
        this.sessionFactory.close();
        StandardServiceRegistryBuilder.destroy(this.serviceRegistry);

        CacheManager cacheManager = this.resolveCacheManager();
        this.getModels().forEach(model -> {
            if (cacheManager.getCache(model.getName(), Object.class, Object.class) != null)
                cacheManager.destroyCache(model.getName());
        });
        // Match the conditional region creation in the constructor: skip destroy when the
        // region was never created (e.g. HAZELCAST_* path with Phase 2d query cache disable).
        // The defensive null checks here would already make this a no-op, but the explicit
        // guard avoids confusing log output on Hazelcast clusters.
        if (cacheManager.getCache("default-update-timestamps-region", Object.class, Object.class) != null)
            cacheManager.destroyCache("default-update-timestamps-region");
        if (cacheManager.getCache("default-query-results-region", Object.class, Object.class) != null)
            cacheManager.destroyCache("default-query-results-region");
    }

    /**
     * Opens a managed {@link Session}, passes it to the consumer, and auto-closes
     * the session when the consumer completes.
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
     * Opens a managed {@link Session}, passes it to the function, returns the result,
     * and auto-closes the session when the function completes.
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

}

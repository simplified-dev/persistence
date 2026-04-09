package dev.simplified.persistence;

import com.google.gson.Gson;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.type.TypeRegistrar;
import dev.simplified.reflection.Reflection;
import dev.simplified.scheduler.Scheduler;
import dev.simplified.util.Logging;
import dev.simplified.util.time.Stopwatch;
import lombok.Cleanup;
import lombok.Getter;
import org.ehcache.core.Ehcache;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
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

    /** Cached repositories keyed by their entity class. */
    private final @NotNull ConcurrentMap<Class<? extends JpaModel>, Repository<? extends JpaModel>> repositories = Concurrent.newMap();

    /** Topologically sorted entity classes discovered from the {@link RepositoryFactory}. */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /** The immutable configuration that produced this session. */
    private final @NotNull JpaConfig config;

    /** Internal scheduler for repository refresh tasks, shut down on {@link #shutdown()}. */
    private final @NotNull Scheduler scheduler;

    /** Gson instance configured with this session's {@link dev.simplified.gson.GsonSettings}. */
    private final @NotNull Gson gson;

    /** Assembled Hibernate and HikariCP connection properties. */
    private final @NotNull ConcurrentMap<String, Object> properties;

    /** Hibernate entity metadata including custom type registrations and column adjustments. */
    private final @NotNull Metadata metadata;

    /** The Hibernate session factory opened from {@link #metadata}. */
    private final @NotNull SessionFactory sessionFactory;

    /** The Hibernate service registry backing {@link #sessionFactory}. */
    private final @NotNull StandardServiceRegistry serviceRegistry;

    /** Timing snapshot of the full constructor bootstrap. */
    private final @NotNull Stopwatch initialization;

    /** {@code true} while this session has not been shut down. */
    private boolean active = true;

    /** Guard flag preventing {@link #cacheRepositories()} from running more than once. */
    private boolean repositoriesCached = false;

    /** Timing snapshot of the {@link #cacheRepositories()} pass, or {@code null} if not yet run. */
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

    /** Creates a JCache configuration with the given name and TTL, reusing existing caches. */
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

    /** Assembles Hibernate and HikariCP properties from the {@link JpaConfig}. */
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

        return properties.toUnmodifiableMap();
    }

    /** Registers annotated entity classes and configures per-entity cache logging. */
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

            for (Class<? extends JpaModel> model : this.models)
                this.repositories.put(model, this.config.getRepositoryFactory().create(this, model));

            this.repositoryCache = Stopwatch.of(startTime);

            // Schedule proactive refresh at the shortest cache interval
            long minIntervalMs = this.repositories.values()
                .stream()
                .mapToLong(repo -> repo.getCacheDuration().toMillis())
                .filter(ms -> ms > 0)
                .min()
                .orElse(0);

            if (minIntervalMs > 0)
                this.scheduler.scheduleAsync(this::refreshAll, minIntervalMs, minIntervalMs, TimeUnit.MILLISECONDS);
        } else
            throw new JpaException("Session has already cached repositories");
    }

    /**
     * Performs a coordinated 3-phase refresh across all due entity types.
     *
     * <p><b>Phase 1</b> - Update data sources in topological order (parents first):
     * calls {@link JpaRepository#refresh(boolean)} which delegates to the entity's
     * {@link Source}. JSON sources merge fresh data into the DB; entities without
     * a source (SQL-managed) are no-ops.</p>
     *
     * <p><b>Phase 2</b> - Remove stale entities in reverse topological order (children first):
     * calls {@link JpaRepository#removeStaleEntities()} to delete DB rows whose IDs
     * were not present in the last {@link JpaRepository#persistToDatabase} call.
     * Only affects types whose strategy set {@code lastLoadedEntities}.</p>
     *
     * <p><b>Phase 3</b> - Warm all due caches: evicts the L2 cache and re-queries the
     * database, creating fresh cache entries whose JCache TTL resets via
     * {@link ModifiedExpiryPolicy}.</p>
     *
     * <p>Each per-type operation is wrapped in a try/catch for best-effort processing;
     * failures are logged and do not prevent other types from being refreshed.</p>
     */
    private void refreshAll() {
        ConcurrentList<Class<? extends JpaModel>> dueModels = Concurrent.newList();

        // Phase 1: Update data sources (topological order, parents first)
        for (Class<? extends JpaModel> model : this.models) {
            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            if (repo.getCacheDuration().isZero())
                continue;

            java.time.Duration elapsed = java.time.Duration.between(repo.getLastRefresh().completedAt(), Instant.now());
            if (elapsed.compareTo(repo.getCacheDuration()) < 0)
                continue;

            try {
                repo.refresh(false);
                dueModels.add(model);
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to refresh data for '%s'", model.getSimpleName());
            }
        }

        // Phase 2: Remove stale entities (reverse topological order, children first)
        ConcurrentList<Class<JpaModel>> allModelsReversed = this.models.reversed();

        for (Class<? extends JpaModel> model : allModelsReversed) {
            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            try {
                repo.removeStaleEntities();
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to remove stale entities for '%s'", model.getSimpleName());
            }
        }

        // Phase 3: Evict L2 entries for due types. Phase 2d removed the eager warm pass
        // (the prior `repo.stream().close()` line) because the L2 entity cache populates as
        // a side effect of any subsequent streaming hydration - see ResearchPack finding A4.
        // The eviction itself is still required because Phase 1 may have replaced rows that
        // older cached entries point to.
        for (Class<? extends JpaModel> model : dueModels) {
            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            try {
                repo.evict();
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to evict cache for '%s'", model.getSimpleName());
            }
        }
    }

    /**
     * Performs an unconditional 3-phase refresh of the specified model subset,
     * bypassing the {@link CacheExpiry} due-check that gates the scheduled
     * {@link #refreshAll()} path.
     *
     * <p>Use this when an external signal (for example the Phase 4c asset poller
     * detecting a {@code skyblock-data} commit change) requires an immediate
     * targeted refresh rather than waiting for the next scheduler tick. The
     * method is idempotent and safe to invoke repeatedly with the same set.
     *
     * <p>Execution mirrors {@link #refreshAll()}:
     * <ol>
     *     <li><b>Phase 1</b> - iterate {@link #models} in topological order and call
     *         {@link JpaRepository#refresh(boolean)} for every model present in {@code targetModels}
     *         that has a registered repository. Models with no source are no-ops.</li>
     *     <li><b>Phase 2</b> - iterate the reversed model list and call
     *         {@link JpaRepository#removeStaleEntities()} on the same target set so
     *         FK-dependent children are removed before parents.</li>
     *     <li><b>Phase 3</b> - iterate the target set and call {@link JpaRepository#evict()}
     *         so subsequent queries repopulate the L2 entity cache from the newly-loaded rows.</li>
     * </ol>
     *
     * <p>Entries in {@code targetModels} that do not match any registered repository
     * are silently skipped - the caller (for example a poller that resolves model classes
     * from a remote manifest) may legitimately reference classes that have been renamed or
     * removed since the last schema revision, and crashing the refresh cycle over a
     * dangling entry would defeat the poller's graceful-degradation design.
     *
     * <p>An empty or {@code null}-element-free target set is a no-op. Per-model failures
     * are wrapped in {@link JpaException} with the model's simple name in the message.
     *
     * @param targetModels the model subset to refresh; entries not registered in this
     *                     session are skipped
     * @throws JpaException if any source reload, stale removal, or cache eviction fails
     */
    public void refreshModels(@NotNull Collection<Class<? extends JpaModel>> targetModels) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        if (targetModels.isEmpty())
            return;

        Set<Class<? extends JpaModel>> targetSet = new HashSet<>(targetModels);

        // Phase 1: Update data sources in topological order, restricted to the target subset.
        ConcurrentList<Class<? extends JpaModel>> refreshedModels = Concurrent.newList();
        for (Class<JpaModel> model : this.models) {
            if (!targetSet.contains(model))
                continue;

            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            try {
                repo.refresh(false);
                refreshedModels.add(model);
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to refresh data for '%s'", model.getSimpleName());
            }
        }

        // Phase 2: Remove stale entities in reverse topological order so children drop before parents.
        for (Class<? extends JpaModel> model : this.models.reversed()) {
            if (!targetSet.contains(model))
                continue;

            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            try {
                repo.removeStaleEntities();
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to remove stale entities for '%s'", model.getSimpleName());
            }
        }

        // Phase 3: Evict the L2 entity region for every model that actually reloaded data.
        // Matches the refreshAll() rationale - subsequent streaming hydration repopulates L2
        // as a side effect, so no eager warm pass is required.
        for (Class<? extends JpaModel> model : refreshedModels) {
            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            try {
                repo.evict();
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to evict cache for '%s'", model.getSimpleName());
            }
        }
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
     * a {@link dev.simplified.collection.tuple.single.LifecycleSingleStream} which auto-closes
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

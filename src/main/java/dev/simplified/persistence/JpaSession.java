package dev.simplified.persistence;

import com.google.gson.Gson;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.type.TypeRegistrar;
import dev.simplified.scheduler.Scheduler;
import dev.simplified.util.LogUtil;
import dev.simplified.collection.concurrent.Concurrent;
import dev.simplified.collection.concurrent.ConcurrentList;
import dev.simplified.collection.concurrent.ConcurrentMap;
import dev.simplified.reflection.Reflection;
import dev.simplified.util.time.Stopwatch;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
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
import java.util.EnumSet;
import java.util.Map;
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
@Log4j2
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

        // Build JCache regions
        this.buildCacheConfiguration("default-update-timestamps-region", Duration.ETERNAL);
        long ttl = config.getQueryResultsTTL();
        Duration queryDuration = ttl <= 0 ? Duration.ETERNAL : new Duration(TimeUnit.SECONDS, ttl);
        this.buildCacheConfiguration("default-query-results-region", queryDuration);

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
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();

        if (cacheManager.getCache(cacheName, Object.class, Object.class) != null)
            return;

        MutableConfiguration<Object, Object> cacheConfiguration = new MutableConfiguration<>()
            .setStoreByValue(false)
            .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(duration));

        cacheManager.createCache(cacheName, cacheConfiguration);
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

        properties.put("hibernate.jdbc.log.warnings", this.config.isLogLevel(Level.WARN));
        properties.put("hibernate.show_sql", this.config.isLogLevel(Level.DEBUG));
        properties.put("hibernate.format_sql", this.config.isLogLevel(Level.TRACE));
        properties.put("hibernate.highlight_sql", this.config.isLogLevel(Level.TRACE));
        properties.put("hibernate.use_sql_comments", this.config.isLogLevel(Level.DEBUG));

        properties.put("hibernate.generate_statistics", this.config.isUsingStatistics());

        properties.put("hibernate.order_inserts", true);
        properties.put("hibernate.order_updates", true);

        properties.put("hibernate.jdbc.batch_size", 100);
        properties.put("hibernate.jdbc.fetch_size", 400);
        properties.put("hibernate.jdbc.use_get_generated_keys", true);

        // Cache
        properties.put("hibernate.cache.region.factory_class", "jcache");
        properties.put("hibernate.cache.use_reference_entries", true);
        properties.put("hibernate.cache.use_structured_entries", this.config.isLogLevel(Level.DEBUG));
        properties.put("hibernate.cache.use_query_cache", this.config.isUsingQueryCache());
        properties.put("hibernate.cache.use_second_level_cache", this.config.isUsing2ndLevelCache());
        properties.put("hibernate.jakarta.cache.missing_cache_strategy", this.config.getMissingCacheStrategy().getExternalRepresentation());

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
            .forEach(modelType -> LogUtil.setLevel(
                String.format("%s-%s", Ehcache.class, modelType.getName()),
                this.config.getLogLevel().name()
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

            java.time.Duration elapsed = java.time.Duration.between(repo.getLastRefresh().getCompletedAt(), Instant.now());
            if (elapsed.compareTo(repo.getCacheDuration()) < 0)
                continue;

            try {
                repo.refresh(false);
                dueModels.add(model);
            } catch (Exception ex) {
                log.warn("Phase 1: Failed to refresh data for {}", model.getSimpleName(), ex);
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
                log.warn("Phase 2: Failed to remove stale entities for {}", model.getSimpleName(), ex);
            }
        }

        // Phase 3: Warm ALL caches (all due types, including SQL)
        for (Class<? extends JpaModel> model : dueModels) {
            Repository<? extends JpaModel> repoIface = this.repositories.get(model);
            if (!(repoIface instanceof JpaRepository<?> repo))
                continue;

            try {
                repo.evict();
                repo.stream().close();
            } catch (Exception ex) {
                log.warn("Phase 3: Failed to warm cache for {}", model.getSimpleName(), ex);
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

        log.info("Exported {} entities to {}", this.models.size(), dbFile);
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

        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();
        this.getModels().forEach(model -> {
            if (cacheManager.getCache(model.getName(), Object.class, Object.class) != null)
                cacheManager.destroyCache(model.getName());
        });
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

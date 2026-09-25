package dev.simplified.persistence.source;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.proxied.ProxiedRow;
import dev.simplified.persistence.refused.RefusedModel;
import jakarta.persistence.criteria.CriteriaQuery;
import org.ehcache.jsr107.EhcacheCachingProvider;
import org.hibernate.SessionFactory;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cache.jcache.internal.JCacheRegionFactory;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.stat.Statistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An opened database: the settings its builder carries reaching Hibernate, a builder missing its
 * driver or its models refused, the rows a read hands back, what a failed open leaves behind, a close
 * that leaves every other open database caching, the shutdown hook that holds an open one, and a close
 * that can be repeated and leaves nothing the JVM holds.
 */
@Tag("slow")
class RelationalSourceTest {

    private static final String DATABASE = "relational_source_settings";

    private static @NotNull CacheManager defaultCacheManager() {
        return Caching.getCachingProvider(EhcacheCachingProvider.class.getName()).getCacheManager();
    }

    private static @NotNull SessionFactory factoryOf(@NotNull RelationalSource database) {
        return database.with(hibernate -> {
            return hibernate.getSessionFactory();
        });
    }

    private static @NotNull CacheManager cacheManagerOf(@NotNull RelationalSource database) {
        RegionFactory regionFactory = factoryOf(database).getCache().unwrap(RegionFactory.class);
        return ((JCacheRegionFactory) regionFactory).getCacheManager();
    }

    @SuppressWarnings("unchecked")
    private static @NotNull Duration lifeOf(@NotNull RelationalSource database, @NotNull String region) {
        Cache<Object, Object> cache = cacheManagerOf(database).getCache(region, Object.class, Object.class);
        CompleteConfiguration<Object, Object> configuration = cache.getConfiguration(CompleteConfiguration.class);
        ExpiryPolicy policy = configuration.getExpiryPolicyFactory().create();
        return policy.getExpiryForCreation();
    }

    @Test
    @DisplayName("every builder setting moved off its default reaches the session factory")
    void builderSettingsReachHibernate() {
        RelationalSource database = H2MemoryDriver.named(DATABASE)
            .isUsingStatistics()
            .withCacheConcurrencyStrategy(CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
            .withCacheMissingStrategy(CacheMissingStrategy.CREATE)
            .withQueryResultsTTL(7)
            .withCacheExpiryMs(5000)
            .withModels(JpaModel.resolveModels(TestParentModel.class))
            .build();

        try {
            SessionFactory factory = factoryOf(database);
            Map<String, Object> properties = factory.getProperties();

            assertThat(String.valueOf(properties.get("hibernate.connection.url")), equalTo("jdbc:h2:mem:" + DATABASE + ";DB_CLOSE_DELAY=-1"));
            assertThat(String.valueOf(properties.get("hibernate.cache.use_query_cache")), equalTo("true"));
            assertThat(String.valueOf(properties.get("hibernate.cache.use_second_level_cache")), equalTo("true"));
            assertThat(String.valueOf(properties.get("hibernate.cache.default_cache_concurrency_strategy")), equalTo("nonstrict-read-write"));
            assertThat(String.valueOf(properties.get("hibernate.javax.cache.missing_cache_strategy")), equalTo("create"));
            assertTrue(factory.getStatistics().isStatisticsEnabled(), "statistics were asked for");

            assertThat(cacheManagerOf(database), not(sameInstance(defaultCacheManager())));
            assertThat(lifeOf(database, "default-query-results-region"), equalTo(new Duration(TimeUnit.SECONDS, 7)));
            assertThat(lifeOf(database, TestParentModel.class.getName()), equalTo(new Duration(TimeUnit.MILLISECONDS, 5_000)));

            assertThat(database.toString(), equalTo("jdbc:h2:mem:" + DATABASE + ";DB_CLOSE_DELAY=-1 (create-drop)"));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a row the query answers as a proxy is read as the entity itself")
    void aProxiedRowIsReadAsTheEntity() {
        RelationalSource database = openProxied("relational_source_proxied");

        try {
            // The query run bare answers the row it reaches second as the proxy the first row's lazy
            // parent put in the session, which is what makes this fixture reach a proxy at all.
            boolean answersAProxy = database.with(hibernate -> {
                CriteriaQuery<ProxiedRow> query = hibernate.getCriteriaBuilder().createQuery(ProxiedRow.class);
                query.select(query.from(ProxiedRow.class));
                return hibernate.createQuery(query).getResultList().stream().anyMatch(HibernateProxy.class::isInstance);
            });
            assertTrue(answersAProxy, "The bare query answers no row as a proxy");

            ConcurrentList<ProxiedRow> rows = database.read(ProxiedRow.class);
            assertThat(rows, hasSize(2));

            for (ProxiedRow row : rows) {
                assertThat(row, not(instanceOf(HibernateProxy.class)));
                assertThat(row.getClass(), equalTo(ProxiedRow.class));
            }
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("a row the query answers as a proxy is keyed by its own id")
    void aProxiedRowIsKeyedByItsId() {
        RelationalSource database = openProxied("relational_source_proxied_keyed");

        try {
            ConcurrentMap<String, ProxiedRow> keyed = JpaModel.keyed(ProxiedRow.class, database.read(ProxiedRow.class));

            assertThat(keyed.keySet(), containsInAnyOrder("1", "2"));
            keyed.forEach((id, row) -> assertThat(String.valueOf(row.getId()), equalTo(id)));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("an open that fails part way leaves no region in the provider's default manager")
    void aFailedOpenLeavesNoRegionInTheDefaultManager() {
        assertThrows(
            RuntimeException.class,
            () -> H2MemoryDriver.named("relational_source_refused")
                .withModels(JpaModel.resolveModels(RefusedModel.class))
                .build()
        );

        // The type's region is created before Hibernate builds the metadata that refuses it, in a
        // manager of the database's own, so none ever reaches the provider's default manager.
        assertThat(defaultCacheManager().getCache(RefusedModel.class.getName(), Object.class, Object.class), nullValue());
    }

    @Test
    @DisplayName("a connection naming no driver, and a database given no models to map, are refused before anything opens")
    void anIncompleteBuilderIsRefused() {
        JpaException noDriver = assertThrows(
            JpaException.class,
            () -> RelationalSource.builder().withUrl("jdbc:h2:mem:relational_source_incomplete").build()
        );
        JpaException noModels = assertThrows(
            JpaException.class,
            () -> H2MemoryDriver.named("relational_source_incomplete").build()
        );

        assertThat(noDriver.getMessage(), equalTo("A connection names no driver"));
        assertThat(noModels.getMessage(), equalTo("A relational source names no models to map"));
    }

    @Test
    @DisplayName("closing one database, with its caches on or off, leaves another database caching")
    void closingOneDatabaseLeavesAnotherCaching() {
        RelationalSource database = openParents("relational_source_kept", true);

        try {
            Statistics statistics = factoryOf(database).getStatistics();
            database.transaction(hibernate -> { hibernate.persist(parent(1, "parent1")); });
            assertThat(findParent(database, 1), notNullValue());

            // Closed with its caches on, a database's session factory closes the manager it was
            // handed, and a read in a fresh session still answers from the other's entity region.
            openParents("relational_source_closed_cached", true).close();

            long hits = statistics.getSecondLevelCacheHitCount();
            TestParentModel found = findParent(database, 1);
            assertThat(found, notNullValue());
            assertThat(found.getName(), equalTo("parent1"));
            assertThat(statistics.getSecondLevelCacheHitCount(), greaterThan(hits));

            // Closed with its caches off, a database closes a manager Hibernate never started, and a
            // write and a cacheable query still reach the other's timestamps and query regions.
            openParents("relational_source_closed_uncached", false).close();

            database.transaction(hibernate -> { hibernate.persist(parent(2, "parent2")); });
            long puts = statistics.getQueryCachePutCount();
            List<TestParentModel> parents = database.with(hibernate -> {
                return hibernate.createQuery("from TestParentModel", TestParentModel.class)
                    .setCacheable(true)
                    .getResultList();
            });
            assertThat(parents, hasSize(2));
            assertThat(statistics.getQueryCachePutCount(), greaterThan(puts));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("closing a database a second time does nothing further")
    void closingTwiceIsSafe() {
        RelationalSource database = openParents("relational_source_closed_twice", true);
        SessionFactory factory = factoryOf(database);

        database.close();
        assertTrue(factory.isClosed(), "the first close closes the session factory");
        assertDoesNotThrow(database::close);
        assertTrue(factory.isClosed(), "the session factory stays closed");
    }

    @Test
    @DisplayName("a closed database is no longer reachable through its shutdown hook")
    void aClosedDatabaseIsCollected() throws InterruptedException {
        ReferenceQueue<RelationalSource> queue = new ReferenceQueue<>();
        WeakReference<RelationalSource> reference = closedDetached(queue);

        assertSame(reference, collected(queue, 50), "A closed database is still reachable");
    }

    @Test
    @DisplayName("an open database stays reachable through its shutdown hook")
    void anOpenDatabaseIsHeldByItsHook() throws InterruptedException {
        ReferenceQueue<RelationalSource> queue = new ReferenceQueue<>();
        WeakReference<RelationalSource> reference = openDetached(queue);

        try {
            // Nothing but the hook holds the database, so a collection that reaches it means the
            // JVM would not close it at exit.
            assertThat("An open database was collected", collected(queue, 10), nullValue());
            assertThat(reference.get(), notNullValue());
        } finally {
            RelationalSource database = reference.get();

            if (database != null)
                database.close();
        }
    }

    /**
     * Runs collections until a reference enqueues, each followed by a 100ms wait for it.
     *
     * @param queue the queue a collected reference enqueues on
     * @param rounds the most collections to run
     * @return the reference that enqueued, or {@code null} when none did
     * @throws InterruptedException if a wait is interrupted
     */
    private static @Nullable Reference<? extends RelationalSource> collected(
        @NotNull ReferenceQueue<RelationalSource> queue,
        int rounds
    ) throws InterruptedException {
        Reference<? extends RelationalSource> collected = null;

        for (int attempt = 0; attempt < rounds && collected == null; attempt++) {
            System.gc();
            collected = queue.remove(100);
        }

        return collected;
    }

    /**
     * Opens a database and closes it, handing back only a weak reference so no strong one outlives
     * this frame.
     *
     * @param queue the queue the reference enqueues on once the database is collected
     * @return a weak reference to the closed database
     */
    private static @NotNull WeakReference<RelationalSource> closedDetached(@NotNull ReferenceQueue<RelationalSource> queue) {
        RelationalSource database = openParents("relational_source_collected", true);
        database.close();
        return new WeakReference<>(database, queue);
    }

    /**
     * Opens a database and leaves it open, handing back only a weak reference so no strong one
     * outlives this frame.
     *
     * @param queue the queue the reference enqueues on once the database is collected
     * @return a weak reference to the open database
     */
    private static @NotNull WeakReference<RelationalSource> openDetached(@NotNull ReferenceQueue<RelationalSource> queue) {
        return new WeakReference<>(openParents("relational_source_held", true), queue);
    }

    private static @NotNull RelationalSource openParents(@NotNull String name, boolean caching) {
        return H2MemoryDriver.named(name)
            .isUsingStatistics()
            .withUsingQueryCache(caching)
            .withUsing2ndLevelCache(caching)
            .withModels(JpaModel.resolveModels(TestParentModel.class))
            .build();
    }

    /**
     * Opens a database mapping only {@link ProxiedRow}, holding two rows that are each the other's
     * parent.
     *
     * <p>Whichever row a read reaches first, its lazy parent puts a proxy for the other row in the
     * session before that row is read, so the query answers the other row as that proxy - in either
     * order the database hands them back in.
     *
     * @param name the in-memory database's name
     * @return the open database
     */
    private static @NotNull RelationalSource openProxied(@NotNull String name) {
        RelationalSource database = H2MemoryDriver.named(name)
            .withModels(JpaModel.resolveModels(ProxiedRow.class))
            .build();

        database.transaction(hibernate -> {
            ProxiedRow first = new ProxiedRow();
            first.setId(1L);
            ProxiedRow second = new ProxiedRow();
            second.setId(2L);
            hibernate.persist(first);
            hibernate.persist(second);
            hibernate.flush();
            first.setParent(second);
            second.setParent(first);
        });

        return database;
    }

    private static @NotNull TestParentModel parent(int id, @NotNull String name) {
        TestParentModel parent = new TestParentModel();
        parent.setId(id);
        parent.setName(name);
        return parent;
    }

    private static @Nullable TestParentModel findParent(@NotNull RelationalSource database, int id) {
        return database.with(hibernate -> {
            return hibernate.find(TestParentModel.class, id);
        });
    }

}

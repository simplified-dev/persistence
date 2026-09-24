package dev.simplified.persistence.source;

import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.refused.RefusedModel;
import dev.simplified.util.Logging;
import org.ehcache.jsr107.EhcacheCachingProvider;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cache.jcache.internal.JCacheRegionFactory;
import org.hibernate.cache.spi.RegionFactory;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An opened database: the settings its builder carries reaching Hibernate, what a failed open leaves
 * behind, and a close that leaves every other open database caching.
 */
@Tag("slow")
class RelationalSourceTest {

    private static final String DATABASE = "relational_source_settings";

    private static @NotNull CacheManager defaultCacheManager() {
        return Caching.getCachingProvider(EhcacheCachingProvider.class.getName()).getCacheManager();
    }

    private static @NotNull CacheManager cacheManagerOf(@NotNull RelationalSource database) {
        RegionFactory regionFactory = database.getSessionFactory().getCache().unwrap(RegionFactory.class);
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
            .withDefaultCacheExpiryMs(5000)
            .open(JpaModel.resolveModels(TestParentModel.class), GsonSettings.defaults().create(), Logging.Level.WARN);

        try {
            Map<String, Object> properties = database.getSessionFactory().getProperties();

            assertThat(String.valueOf(properties.get("hibernate.connection.url")), equalTo("jdbc:h2:mem:" + DATABASE + ";DB_CLOSE_DELAY=-1"));
            assertThat(String.valueOf(properties.get("hibernate.cache.use_query_cache")), equalTo("true"));
            assertThat(String.valueOf(properties.get("hibernate.cache.use_second_level_cache")), equalTo("true"));
            assertThat(String.valueOf(properties.get("hibernate.cache.default_cache_concurrency_strategy")), equalTo("nonstrict-read-write"));
            assertThat(String.valueOf(properties.get("hibernate.javax.cache.missing_cache_strategy")), equalTo("create"));
            assertTrue(database.getSessionFactory().getStatistics().isStatisticsEnabled(), "statistics were asked for");

            assertThat(cacheManagerOf(database), not(sameInstance(defaultCacheManager())));
            assertThat(lifeOf(database, "default-query-results-region"), equalTo(new Duration(TimeUnit.SECONDS, 7)));
            assertThat(lifeOf(database, TestParentModel.class.getName()), equalTo(new Duration(TimeUnit.MILLISECONDS, 10_000)));

            assertThat(database.toString(), equalTo("jdbc:h2:mem:" + DATABASE + ";DB_CLOSE_DELAY=-1 (create-drop)"));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("an open that fails part way leaves no region in the provider's default manager")
    void aFailedOpenReleasesWhatItAcquired() {
        assertThrows(
            RuntimeException.class,
            () -> H2MemoryDriver.named("relational_source_refused")
                .open(JpaModel.resolveModels(RefusedModel.class), GsonSettings.defaults().create(), Logging.Level.WARN)
        );

        // The type's region is created before Hibernate builds the metadata that refuses it, in a
        // manager of the database's own, so none ever reaches the provider's default manager.
        assertThat(defaultCacheManager().getCache(RefusedModel.class.getName(), Object.class, Object.class), nullValue());
    }

    @Test
    @DisplayName("closing one database, with its caches on or off, leaves another database caching")
    void closingOneDatabaseLeavesAnotherCaching() {
        RelationalSource database = openParents("relational_source_kept", true);

        try {
            Statistics statistics = database.getSessionFactory().getStatistics();
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

    private static @NotNull RelationalSource openParents(@NotNull String name, boolean caching) {
        return H2MemoryDriver.named(name)
            .isUsingStatistics()
            .isUsingQueryCache(caching)
            .isUsing2ndLevelCache(caching)
            .open(JpaModel.resolveModels(TestParentModel.class), GsonSettings.defaults().create(), Logging.Level.WARN);
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

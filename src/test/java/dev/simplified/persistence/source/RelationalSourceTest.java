package dev.simplified.persistence.source;

import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.refused.RefusedModel;
import dev.simplified.util.Logging;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An opened database: the settings its builder carries reaching Hibernate, and what a failed open
 * leaves behind.
 */
@Tag("slow")
class RelationalSourceTest {

    private static final String DATABASE = "relational_source_settings";

    private static @NotNull CacheManager cacheManager() {
        return Caching.getCachingProvider(JpaCacheProvider.EHCACHE.getProviderClassName()).getCacheManager();
    }

    @SuppressWarnings("unchecked")
    private static @NotNull Duration lifeOf(@NotNull String region) {
        Cache<Object, Object> cache = cacheManager().getCache(region, Object.class, Object.class);
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

            assertThat(lifeOf("default-query-results-region"), equalTo(new Duration(TimeUnit.SECONDS, 7)));
            assertThat(lifeOf(TestParentModel.class.getName()), equalTo(new Duration(TimeUnit.MILLISECONDS, 10_000)));

            assertThat(database.toString(), equalTo("jdbc:h2:mem:" + DATABASE + ";DB_CLOSE_DELAY=-1 (create-drop)"));
        } finally {
            database.close();
        }
    }

    @Test
    @DisplayName("an open that fails part way leaves no cache region behind")
    void aFailedOpenReleasesWhatItAcquired() {
        assertThrows(
            RuntimeException.class,
            () -> H2MemoryDriver.named("relational_source_refused")
                .open(JpaModel.resolveModels(RefusedModel.class), GsonSettings.defaults().create(), Logging.Level.WARN)
        );

        // The type's region is created before Hibernate builds the metadata that refuses it, so only
        // a release on failure takes it away again.
        assertThat(cacheManager().getCache(RefusedModel.class.getName(), Object.class, Object.class), nullValue());
    }

}

package dev.simplified.persistence;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.cache.Caching;

/**
 * Selects the JCache (JSR-107) {@code CachingProvider} implementation that backs the
 * Hibernate second-level cache for a {@link JpaConfig}.
 *
 * <p>Each constant carries the fully-qualified provider class name passed to
 * {@link Caching#getCachingProvider(String)} and an optional XML config
 * resource URI. When the URI is {@code null}, the provider's default configuration
 * is used. When non-null, it must resolve on the runtime classpath.</p>
 *
 * <p>The provider class itself only needs to be on the runtime classpath - the
 * persistence library declares all Hazelcast classes as {@code compileOnly} so
 * consumers that stay on EhCache do not transitively pull Hazelcast.</p>
 *
 * @see JpaConfig.Builder#withCacheProvider(JpaCacheProvider)
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum JpaCacheProvider {

    /** EhCache 3 JSR-107 provider - the default and the only provider on the classpath today. */
    EHCACHE("org.ehcache.jsr107.EhcacheCachingProvider", null),

    /** Hazelcast client-mode provider - connects to a remote cluster, auto-discovers {@code hazelcast-client.xml} from the classpath. */
    HAZELCAST_CLIENT("com.hazelcast.client.cache.impl.HazelcastClientCachingProvider", null),

    /** Hazelcast embedded-mode provider - bootstraps an in-process member, auto-discovers {@code hazelcast.xml} from the classpath. */
    HAZELCAST_EMBEDDED("com.hazelcast.cache.impl.HazelcastServerCachingProvider", null);

    /** Fully-qualified JSR-107 {@code CachingProvider} class name passed to {@link javax.cache.Caching#getCachingProvider(String)}. */
    private final @NotNull String providerClassName;

    /** Classpath resource URI for the provider's XML config, or {@code null} to use the provider's default. */
    private final @Nullable String configUri;

}

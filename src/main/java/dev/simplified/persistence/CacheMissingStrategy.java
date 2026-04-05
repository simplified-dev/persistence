package dev.simplified.persistence;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Controls Hibernate's behavior when a second-level cache region is not explicitly configured.
 */
@Getter
@RequiredArgsConstructor
public enum CacheMissingStrategy {

    /**
     * Fail with an exception on missing caches.
     */
    FAIL("fail"),

    /**
     * Create a new cache when a cache is not found,
     * without logging any warning about the missing cache.
     * <p>
     * Note that caches created this way may be very badly configured (unlimited size and no eviction in particular)
     * unless the cache provider was explicitly configured to use an appropriate configuration for default caches.
     * <p>
     * Ehcache in particular allows setting such default configuration using cache templates.
     *
     * @see <a href="http://www.ehcache.org/documentation/3.0/107.html#supplement-jsr-107-configurations">Supplement JSR-107’s configurations</a>
     */
    CREATE("create"),

    /**
     * Create a new cache when a cache is not found (see {@link #CREATE}),
     * and also log a warning about the missing cache.
     */
    CREATE_WARN("create-warn");

    private final @NotNull String externalRepresentation;

}

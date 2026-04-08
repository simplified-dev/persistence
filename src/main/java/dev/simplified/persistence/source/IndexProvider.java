package dev.simplified.persistence.source;

import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * Loads a {@link ManifestIndex} describing the files available from a remote data source.
 *
 * <p>Implementations typically hit an HTTP endpoint (GitHub Contents API, raw CDN, local
 * filesystem, etc.) and parse the response into the {@code data/v1/index.json} schema
 * captured by {@link ManifestIndex}. Phase 4a ships this interface only; the first concrete
 * implementation (GitHub-backed) lands in Phase 4b inside {@code simplified-data}.
 *
 * <p>Consumers should treat every call as potentially expensive - {@link RemoteJsonSource}
 * calls this on every {@code load()} invocation to avoid stale caching semantics, so
 * implementations are responsible for any caching they need.
 *
 * @see ManifestIndex
 * @see RemoteJsonSource
 */
@FunctionalInterface
public interface IndexProvider {

    /**
     * Loads the manifest index describing the files available from this source.
     *
     * @return the parsed manifest
     * @throws JpaException if the manifest cannot be fetched or parsed
     */
    @NotNull ManifestIndex loadIndex() throws JpaException;

}

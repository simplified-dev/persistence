package dev.simplified.persistence.source;

import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * Fetches the raw UTF-8 text contents of a single file identified by the path recorded in
 * a {@link ManifestIndex.Entry}.
 *
 * <p>Implementations pair with an {@link IndexProvider} to feed a {@link RemoteJsonSource}.
 * Phase 4a ships this interface only; the first concrete implementation (GitHub raw content)
 * lands in Phase 4b inside {@code simplified-data}.
 *
 * @see IndexProvider
 * @see RemoteJsonSource
 */
@FunctionalInterface
public interface FileFetcher {

    /**
     * Fetches the raw UTF-8 text content of the given path.
     *
     * @param path the path from a {@link ManifestIndex.Entry}, as reported by the index
     * @return the file content as a UTF-8 string
     * @throws JpaException if the file cannot be fetched or decoded
     */
    @NotNull String fetchFile(@NotNull String path) throws JpaException;

}

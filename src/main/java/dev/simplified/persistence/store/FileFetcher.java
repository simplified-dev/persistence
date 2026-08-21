package dev.simplified.persistence.store;

import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * Fetches the raw UTF-8 text contents of a single file identified by the path recorded in
 * a {@link ManifestIndex.Entry}.
 *
 * <p>Implementations read one path at a time and hold no manifest of their own, so an
 * {@link EntityStore} that loads from documents pairs a fetcher with whatever names the paths.
 *
 * @see EntityStore
 * @see ManifestIndex
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

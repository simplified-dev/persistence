package dev.simplified.persistence.store;

import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

/**
 * Reads the raw UTF-8 text of one {@link ManifestIndex.Layer} path.
 *
 * <p>An implementation reads one path at a time and holds no catalogue of its own, so where the
 * layers come from is the only thing that differs between a GitHub corpus and a directory on disk.
 * {@link Source#documents} pairs a fetcher with whatever names the paths.
 *
 * @see Source#documents
 * @see ManifestIndex
 */
@FunctionalInterface
public interface FileFetcher {

    /**
     * Fetches the raw UTF-8 text content of the given path.
     *
     * @param path a layer path, relative to the origin's root
     * @return the file content as a UTF-8 string
     * @throws JpaException if the file cannot be fetched or decoded
     */
    @NotNull String fetchFile(@NotNull String path) throws JpaException;

}

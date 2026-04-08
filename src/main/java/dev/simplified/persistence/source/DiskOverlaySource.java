package dev.simplified.persistence.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A {@link Source} wrapper that prefers a local filesystem overlay and falls back to an
 * inner source when the overlay is missing.
 *
 * <p>Intended for the dev-mode workflow where a contributor edits
 * {@code skyblock-data/data/v1/items/items.json} locally and wants {@code simplified-data}
 * to pick up the local copy without pushing to GitHub. Production deployments should not
 * wrap their {@link RemoteJsonSource} in this class.
 *
 * <p>On every {@link #load(JpaRepository)} call this source:
 * <ol>
 *   <li>checks whether the overlay path exists and is readable,</li>
 *   <li>if yes, reads and parses it fresh (NO caching, NO hashing),</li>
 *   <li>if no, delegates to the inner source.</li>
 * </ol>
 *
 * <p><b>Do not add a content-hash cache, a modification-time cache, or a
 * {@link java.nio.file.WatchService} listener inside this class.</b> The locked
 * architecture decision (see Phase 4 memory entry) is to let Phase 4c's scheduled
 * content-hash poller drive refresh cadence - not a per-file listener. The
 * {@code java.nio.file.WatchService} API has several documented Windows bugs
 * (JDK-8014394, JDK-8029516, duplicate {@code ENTRY_MODIFY}) that make it unsuitable
 * for the dev OS. Any cache added here would shadow the poller's change detection and
 * break the Phase 4c delta engine. Every {@code load()} call re-reads from disk; the
 * calling {@link JpaRepository#refresh} controls cadence.
 *
 * @param <T> the entity type
 * @see RemoteJsonSource
 */
@Getter
public final class DiskOverlaySource<T extends JpaModel> implements Source<T> {

    private final @NotNull Source<T> inner;
    private final @NotNull Path overlayPath;
    private final @NotNull Class<T> modelClass;

    /**
     * Constructs a new disk overlay wrapper.
     *
     * @param inner the fallback source consulted when the overlay file is absent
     * @param overlayPath the local filesystem path checked on every load
     * @param modelClass the entity class handled by the inner source, passed explicitly so
     *                   parsing can happen before the inner source is consulted
     */
    public DiskOverlaySource(
        @NotNull Source<T> inner,
        @NotNull Path overlayPath,
        @NotNull Class<T> modelClass
    ) {
        this.inner = inner;
        this.overlayPath = overlayPath;
        this.modelClass = modelClass;
    }

    @Override
    public @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) throws JpaException {
        if (Files.exists(this.overlayPath) && Files.isReadable(this.overlayPath)) {
            try {
                String text = Files.readString(this.overlayPath);
                Gson gson = repository.getSession().getGson();
                Type listType = TypeToken.getParameterized(ConcurrentList.class, repository.getType()).getType();
                ConcurrentList<T> loaded = gson.fromJson(text, listType);

                if (loaded != null)
                    return loaded;
            } catch (Exception ex) {
                throw new JpaException(ex, "Failed to read disk overlay for '%s' at '%s'", this.modelClass.getName(), this.overlayPath);
            }
        }

        return this.inner.load(repository);
    }

}

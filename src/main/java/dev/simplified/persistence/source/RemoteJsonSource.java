package dev.simplified.persistence.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;

/**
 * A read-only {@link Source} that loads entities from a remote JSON manifest.
 *
 * <p>On every {@link #load(JpaRepository)} call this source:
 * <ol>
 *   <li>asks its {@link IndexProvider} for the current {@link ManifestIndex},</li>
 *   <li>finds the entry whose {@link ManifestIndex.Entry#getModelClass model class} equals
 *       the repository's entity class FQCN,</li>
 *   <li>asks its {@link FileFetcher} for the raw UTF-8 text of the primary file,</li>
 *   <li>parses the text into a {@code ConcurrentList<T>} via the repository session's
 *       {@link Gson} instance,</li>
 *   <li>if the entry carries an {@code _extra} companion, fetches and parses it and
 *       appends the entries (matching {@link JsonSource}'s merge semantics - no dedup).</li>
 * </ol>
 *
 * <p>Phase 4a intentionally performs zero caching. Every call hits the {@link IndexProvider}
 * and {@link FileFetcher}; implementations of those interfaces are responsible for their own
 * caching. Phase 4c will layer content-hash change detection on top via the
 * {@code ExternalAssetState} / {@code ExternalAssetEntryState} tables and a scheduled poller.
 *
 * <p>Phase 4a does NOT ship a GitHub-backed {@link IndexProvider} or {@link FileFetcher} -
 * those land in Phase 4b inside {@code simplified-data}. Tests construct this source with
 * in-memory stubs.
 *
 * @param <T> the entity type
 * @see IndexProvider
 * @see FileFetcher
 * @see ManifestIndex
 */
@Getter
public final class RemoteJsonSource<T extends JpaModel> implements Source<T> {

    private final @NotNull String sourceId;
    private final @NotNull IndexProvider indexProvider;
    private final @NotNull FileFetcher fileFetcher;
    private final @NotNull Class<T> modelClass;

    /**
     * Constructs a new remote JSON source.
     *
     * @param sourceId the human-readable identifier for this source (e.g. {@code "skyblock-data"}),
     *                 used in exception messages and matching the {@code ExternalAssetState.sourceId}
     *                 column in Phase 4c
     * @param indexProvider the manifest provider
     * @param fileFetcher the per-file fetcher
     * @param modelClass the entity class handled by this source
     */
    public RemoteJsonSource(
        @NotNull String sourceId,
        @NotNull IndexProvider indexProvider,
        @NotNull FileFetcher fileFetcher,
        @NotNull Class<T> modelClass
    ) {
        this.sourceId = sourceId;
        this.indexProvider = indexProvider;
        this.fileFetcher = fileFetcher;
        this.modelClass = modelClass;
    }

    @Override
    public @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) throws JpaException {
        try {
            ManifestIndex manifest = this.indexProvider.loadIndex();
            ManifestIndex.Entry entry = manifest.getFiles()
                .stream()
                .filter(e -> e.getModelClass().equals(this.modelClass.getName()))
                .findFirst()
                .orElseThrow(() -> new JpaException(
                    "No manifest entry for '%s' under source '%s'",
                    this.modelClass.getName(),
                    this.sourceId
                ));

            Gson gson = repository.getSession().getGson();
            Type listType = TypeToken.getParameterized(ConcurrentList.class, repository.getType()).getType();

            String primaryText = this.fileFetcher.fetchFile(entry.getPath());
            ConcurrentList<T> loaded = gson.fromJson(primaryText, listType);

            if (loaded == null)
                loaded = Concurrent.newList();

            if (entry.isHasExtra() && entry.getExtraPath() != null) {
                String extraText = this.fileFetcher.fetchFile(entry.getExtraPath());
                ConcurrentList<T> extras = gson.fromJson(extraText, listType);

                if (extras != null)
                    loaded.addAll(extras);
            }

            return loaded;
        } catch (JpaException jpaEx) {
            throw jpaEx;
        } catch (Exception ex) {
            throw new JpaException(ex, "Failed to load '%s' from source '%s'", this.modelClass.getName(), this.sourceId);
        }
    }

}

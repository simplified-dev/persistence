package dev.simplified.persistence.source;

import com.google.gson.reflect.TypeToken;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.util.StringUtil;
import dev.simplified.util.SystemUtil;
import jakarta.persistence.Table;
import lombok.Cleanup;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Optional;

/**
 * A {@link Source} that loads entities from JSON classpath resources.
 *
 * <p>The resource path is built from {@code {resourceBase}/{@Table.name}.json}. Supplemental
 * entries from a {@code _extra} suffixed resource are merged if present.
 *
 * @param <T> the entity type
 */
final class JsonSource<T extends JpaModel> implements Source<T> {

    private final @NotNull String resourceBase;

    /**
     * Constructs a new source that loads JSON from the given classpath base directory.
     *
     * @param resourceBase the classpath prefix (e.g. {@code "skyblock"})
     */
    JsonSource(@NotNull String resourceBase) {
        this.resourceBase = resourceBase;
    }

    @Override
    public @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) throws JpaException {
        try {
            @Cleanup InputStream stream = this.openJsonStream(repository, "");

            if (stream == null)
                return Concurrent.newList();

            @Cleanup InputStreamReader reader = new InputStreamReader(stream);
            Type listType = TypeToken.getParameterized(ConcurrentList.class, repository.getType()).getType();
            ConcurrentList<T> loaded = repository.getSession().getGson().fromJson(reader, listType);

            if (loaded == null)
                loaded = Concurrent.newList();

            // Load supplemental entries if available
            InputStream extraStream = this.openJsonStream(repository, "_extra");

            if (extraStream != null) {
                @Cleanup InputStreamReader extraReader = new InputStreamReader(extraStream);
                ConcurrentList<T> extras = repository.getSession().getGson().fromJson(extraReader, listType);

                if (extras != null)
                    loaded.addAll(extras);
            }

            return loaded;
        } catch (JpaException jpaEx) {
            throw jpaEx;
        } catch (Exception ex) {
            throw new JpaException(ex);
        }
    }

    /**
     * Opens an {@link InputStream} to a suffixed JSON classpath resource for the given repository's entity.
     *
     * @param repository the repository whose entity type determines the resource name
     * @param suffix the suffix to append to the table name (e.g. {@code "_extra"} or {@code ""})
     * @return the input stream, or {@code null} if the resource does not exist
     */
    private @Nullable InputStream openJsonStream(@NotNull JpaRepository<T> repository, @NotNull String suffix) {
        return SystemUtil.getResource(this.buildJsonResourcePath(repository, suffix));
    }

    /**
     * Builds the classpath resource path for the given repository's entity JSON file.
     *
     * @param repository the repository whose entity type determines the resource name
     * @param suffix the suffix to append (e.g. {@code "_extra"} or {@code ""})
     * @return the resolved resource path
     */
    private @NotNull String buildJsonResourcePath(@NotNull JpaRepository<T> repository, @NotNull String suffix) {
        Table table = Optional.ofNullable(repository.getType().getAnnotation(Table.class))
            .orElseThrow(() -> new JpaException("JSON-backed JpaModel requires @Table annotation: %s", repository.getType().getName()));

        String base = this.resourceBase.replaceAll("/$", "");
        String name = String.format("%s%s.json", table.name().replaceAll("\\.json$", ""), suffix);
        return StringUtil.isEmpty(base) ? name : String.format("%s/%s", base, name);
    }

}

package dev.simplified.persistence.store;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.function.Supplier;

/**
 * Where a type's rows come from, whether that is a relational table, a JSON document or anything else.
 *
 * <p>One source serves every type an origin publishes, so the type is an argument rather than a type
 * parameter. A registry of one source per model is the shape that grows with the corpus; this one does
 * not.
 *
 * <p>Reading is all a source promises. Writing is {@link Writable}, and a source that was handed no
 * write instruction simply is not one - which is how an origin a caller may read but not update is
 * expressed in the type system rather than in a document.
 *
 * @see Writable
 */
public interface Source {

    /**
     * Reads every row the origin holds for the given type.
     *
     * @param type the entity class to read
     * @param <T> the entity type
     * @return the rows, empty when the origin holds none
     * @throws JpaException if the read fails
     */
    <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException;

    /**
     * Returns the source for rows the database itself authors.
     *
     * <p>It reads nothing, because there is no external origin to read from. Every registered type has
     * a source this way, so nothing has to special-case the absence of one.
     *
     * @return a source that holds no rows
     */
    static @NotNull Source none() {
        return None.INSTANCE;
    }

    /**
     * Returns a source reading each type out of the layers a catalogue names for it.
     *
     * <p>A type names its document through the table name it already declares, the catalogue names
     * that document's layers, and the layers merge by key with the later one winning. That is one
     * mechanism for a generated file, its companion overrides and a local overlay, rather than three.
     *
     * @param manifest the catalogue of the origin's documents
     * @param fetcher reads one layer's bytes
     * @param gson the instance documents are parsed with
     * @return a source reading documents through the catalogue
     */
    static @NotNull Source documents(
        @NotNull Supplier<ManifestIndex> manifest,
        @NotNull FileFetcher fetcher,
        @NotNull Gson gson
    ) {
        return new Documents(manifest, fetcher, gson);
    }

    /**
     * Holder for the source of rows the database itself authors.
     */
    final class None implements Source {

        private static final @NotNull Source INSTANCE = new None();

        private None() {}

        /** {@inheritDoc} */
        @Override
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
            return Concurrent.newUnmodifiableList();
        }

    }

    /**
     * A source reading each type out of the layers a {@link ManifestIndex} names for it.
     */
    final class Documents implements Source {

        private final @NotNull Supplier<ManifestIndex> manifest;
        private final @NotNull FileFetcher fetcher;
        private final @NotNull Gson gson;

        private Documents(@NotNull Supplier<ManifestIndex> manifest, @NotNull FileFetcher fetcher, @NotNull Gson gson) {
            this.manifest = manifest;
            this.fetcher = fetcher;
            this.gson = gson;
        }

        /** {@inheritDoc} */
        @Override
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException {
            String name = JpaModel.documentOf(type);
            ConcurrentList<ManifestIndex.Layer> layers = this.manifest.get().layersOf(name);

            if (layers.isEmpty())
                throw new JpaException("The origin names no document '%s' for '%s'", name, type.getName());

            Type listType = TypeToken.getParameterized(ConcurrentList.class, type).getType();
            ConcurrentList<T> read = Concurrent.newList();

            for (ManifestIndex.Layer layer : layers) {
                ConcurrentList<T> rows = this.gson.fromJson(this.fetcher.fetchFile(layer.path()), listType);

                if (rows != null)
                    read.addAll(rows);
            }

            // Insertion order is the first layer's order, and a later layer repeating a key replaces
            // that row in place rather than appending a second one. That is what makes a companion
            // file an override of the generated one rather than a second copy of it.
            return Concurrent.newUnmodifiableList(JpaModel.keyed(type, read).values());
        }

    }

    /**
     * The write half, for an origin a caller holds instructions to update.
     */
    interface Writable extends Source {

        /**
         * Applies one write to the origin.
         *
         * <p>Granularity is the origin's concern. A document source reads its current layers, applies
         * the request and rewrites the file; a relational source applies the rows one at a time.
         * Neither leaks into the request.
         *
         * @param request the write to apply
         * @param <T> the entity type
         * @throws JpaException if the write fails, including when the request's precondition no longer
         *         holds
         */
        <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException;

    }

}

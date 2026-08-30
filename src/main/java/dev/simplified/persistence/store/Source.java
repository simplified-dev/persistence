package dev.simplified.persistence.store;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;

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
 * @see DocumentOrigin
 * @see RelationalOrigin
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
     * Returns a source reading each type out of the layers an origin names for it.
     *
     * <p>A type names its document through the table name it already declares, the origin names that
     * document's layers, and the layers merge by key with the later one winning. That is one mechanism
     * for a generated file, its companion overrides and a local overlay, rather than three.
     *
     * @param origin the tree of files the layers are read out of
     * @param gson the instance documents are parsed with
     * @return a source reading documents off that origin
     */
    static @NotNull Source documents(@NotNull DocumentOrigin origin, @NotNull Gson gson) {
        return new Documents(origin, gson);
    }

    /**
     * Returns a source reading and writing each type through an origin a caller may update.
     *
     * <p>The write instruction is the origin's, so it is the origin's type that carries it. Handing
     * this a read-only origin does not compile, which is what keeps a caller holding no instruction
     * from building a source that claims one.
     *
     * @param origin the tree of files the layers are read out of and written back to
     * @param gson the instance documents are parsed and serialized with
     * @return a source reading and writing documents off that origin
     */
    static @NotNull Writable documents(@NotNull DocumentOrigin.Writable origin, @NotNull Gson gson) {
        return new WritableDocuments(origin, gson);
    }

    /**
     * A source reading each type out of the layers a {@link DocumentOrigin} names for it.
     */
    class Documents implements Source {

        final @NotNull DocumentOrigin origin;
        final @NotNull Gson gson;

        Documents(@NotNull DocumentOrigin origin, @NotNull Gson gson) {
            this.origin = origin;
            this.gson = gson;
        }

        /** {@inheritDoc} */
        @Override
        public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException {
            return Concurrent.newUnmodifiableList(this.merge(type, this.layers(type)).values());
        }

        /**
         * The paths a type's document is made of, in merge order.
         *
         * @param type the entity class
         * @return the paths, never empty
         * @throws JpaException if the origin publishes no document under the type's name
         */
        final @NotNull ConcurrentList<String> layers(@NotNull Class<? extends JpaModel> type) throws JpaException {
            String name = JpaModel.documentOf(type);
            ConcurrentList<String> layers = this.origin.layersOf(name);

            if (layers.isEmpty())
                throw new JpaException("The origin names no document '%s' for '%s'", name, type.getName());

            return layers;
        }

        /**
         * Reads every layer of a type's document and keys the result.
         *
         * <p>Insertion order is the first layer's order, and a later layer repeating a key replaces
         * that row in place rather than appending a second one. That is what makes a companion file
         * an override of the generated one rather than a second copy of it.
         *
         * @param type the entity class
         * @param layers the paths to read, in merge order
         * @param <T> the entity type
         * @return the merged rows, keyed by their id
         * @throws JpaException if a layer cannot be read
         */
        final <T extends JpaModel> @NotNull ConcurrentMap<String, T> merge(
            @NotNull Class<T> type,
            @NotNull ConcurrentList<String> layers
        ) throws JpaException {
            Type listType = TypeToken.getParameterized(ConcurrentList.class, type).getType();
            ConcurrentList<T> read = Concurrent.newList();

            for (String path : layers) {
                ConcurrentList<T> rows = this.gson.fromJson(this.origin.read(path), listType);

                if (rows != null)
                    read.addAll(rows);
            }

            return JpaModel.keyed(type, read);
        }

    }

    /**
     * A document source over an origin a caller holds instructions to update.
     */
    final class WritableDocuments extends Documents implements Writable {

        private final @NotNull DocumentOrigin.Writable writes;

        WritableDocuments(@NotNull DocumentOrigin.Writable origin, @NotNull Gson gson) {
            super(origin, gson);
            this.writes = origin;
        }

        /**
         * {@inheritDoc}
         *
         * <p>A document is a whole file, so a write is: read the layers, apply the rows to the merged
         * result, and rewrite the first layer carrying all of it. Granularity is the origin's problem
         * rather than the caller's, and here the origin's granularity is the file.
         */
        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
            if (request.rows().isEmpty())
                return;

            ConcurrentList<String> layers = this.layers(request.type());
            ConcurrentMap<String, T> merged = this.merge(request.type(), layers);
            ConcurrentMap<String, T> applied = JpaModel.keyed(request.type(), request.rows());

            if (request.operation() == WriteRequest.Operation.DELETE)
                applied.keySet().forEach(merged::remove);
            else
                merged.putAll(applied);

            Type listType = TypeToken.getParameterized(ConcurrentList.class, request.type()).getType();

            this.writes.write(
                layers.getFirst(),
                this.gson.toJson(Concurrent.newUnmodifiableList(merged.values()), listType),
                request.getPrecondition()
            );
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

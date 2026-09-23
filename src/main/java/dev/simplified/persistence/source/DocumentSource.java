package dev.simplified.persistence.source;

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
 * A source reading each type out of the layers a {@link DocumentOrigin} names for it.
 *
 * <p>A type names its document through the table name it already declares, the origin names that
 * document's layers, and the layers merge by key with the later one winning. That is one mechanism
 * for a generated file, its companion overrides and a local overlay, rather than three.
 *
 * <p>Reading is all this promises. An origin a caller holds instructions to update is read and
 * written through {@link DocumentSource.Writable}.
 */
public sealed class DocumentSource implements Source {

    /**
     * The tree of files the layers are read out of.
     */
    final @NotNull DocumentOrigin origin;

    /**
     * The instance documents are parsed with.
     */
    final @NotNull Gson gson;

    /**
     * Constructs a source reading documents off the given origin.
     *
     * @param origin the tree of files the layers are read out of
     * @param gson the instance documents are parsed with
     */
    public DocumentSource(@NotNull DocumentOrigin origin, @NotNull Gson gson) {
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

    /**
     * A document source over an origin a caller holds instructions to update.
     *
     * <p>The write instruction is the origin's, so it is the origin's type that carries it: this takes
     * a {@link DocumentOrigin.Writable}, and handing it a read-only origin does not compile. That is
     * what keeps a caller holding no instruction from building a source that claims one.
     */
    public static final class Writable extends DocumentSource implements Source.Writable {

        /**
         * The origin, as the type that carries its write instruction.
         */
        private final @NotNull DocumentOrigin.Writable writes;

        /**
         * Constructs a source reading and writing documents off the given origin.
         *
         * @param origin the tree of files the layers are read out of and written back to
         * @param gson the instance documents are parsed and serialized with
         */
        public Writable(@NotNull DocumentOrigin.Writable origin, @NotNull Gson gson) {
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

}

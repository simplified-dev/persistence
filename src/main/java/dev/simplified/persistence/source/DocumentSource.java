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
import java.util.Map;

/**
 * A source reading each type out of the layers a {@link DocumentOrigin} names for it.
 *
 * <p>A type names its document through the table name it already declares, the origin names that
 * document's layers, and the layers merge by key with the later one winning. That is one mechanism
 * for a generated file, its companion overrides and a local overlay, rather than three. A type's
 * fingerprint is its document's, which the origin answers when it can.
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

    /**
     * {@inheritDoc}
     *
     * <p>The layers fold in merge order into one insertion-ordered map, so the first layer sets the
     * order and a later layer repeating a key replaces that row in place rather than appending a
     * second one. That is what makes a companion file an override of the generated one rather than a
     * second copy of it.
     */
    @Override
    public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) throws JpaException {
        ConcurrentMap<String, T> merged = Concurrent.newLinkedMap();

        for (String path : this.layers(type))
            merged.putAll(this.rowsIn(type, this.origin.read(path)));

        return Concurrent.newUnmodifiableList(merged.values());
    }

    /**
     * {@inheritDoc}
     *
     * <p>The origin is asked once, and each type answers the fingerprint of the document its table
     * names. A type whose document the origin does not fingerprint is left out.
     */
    @Override
    public @NotNull ConcurrentMap<Class<? extends JpaModel>, String> fingerprints(
        @NotNull ConcurrentList<Class<JpaModel>> types
    ) throws JpaException {
        ConcurrentMap<String, String> documents = this.origin.fingerprints();
        ConcurrentMap<Class<? extends JpaModel>, String> fingerprints = Concurrent.newMap();

        if (documents.isEmpty())
            return fingerprints;

        for (Class<JpaModel> type : types) {
            String fingerprint = documents.get(JpaModel.documentOf(type));

            if (fingerprint != null)
                fingerprints.put(type, fingerprint);
        }

        return fingerprints;
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
     * Parses one layer of a type's document and keys its rows.
     *
     * @param type the entity class
     * @param text the layer's content
     * @param <T> the entity type
     * @return the layer's rows keyed by their id, in the layer's order
     */
    final <T extends JpaModel> @NotNull ConcurrentMap<String, T> rowsIn(@NotNull Class<T> type, @NotNull String text) {
        Type listType = TypeToken.getParameterized(ConcurrentList.class, type).getType();
        ConcurrentList<T> rows = this.gson.fromJson(text, listType);
        return rows == null ? Concurrent.newLinkedMap() : JpaModel.keyed(type, rows);
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
         * <p>A write rewrites the layer that owns each row it names, adds a row no layer carries to the
         * last layer, and removes a deleted key from every layer. A layer it does not change is not
         * written. A key's owner is the last layer carrying it, which is the one a read answers, and a
         * new row goes last so that a regenerated first layer cannot drop it.
         *
         * <p>Which layer a key goes to is decided from the layers as this source reads them. Each
         * changed layer is then one {@linkplain DocumentOrigin.Writable#edit edit} of the origin,
         * which applies only the rows routed to that layer to its text as the origin holds it when it
         * writes, so a row committed to the layer in between survives beside the written one, and a
         * layer that moves under the origin's own read has the edit refused rather than written over
         * it. Granularity is the origin's problem rather than the caller's. Changed layers are
         * edited in merge order, so a delete that fails between two layers leaves the later layer's
         * row, which is what a read answered before the write, rather than an older one.
         */
        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
            if (request.rows().isEmpty())
                return;

            Class<T> type = request.type();
            boolean delete = request.operation() == WriteRequest.Operation.DELETE;
            ConcurrentList<String> paths = this.layers(type);
            ConcurrentMap<String, ConcurrentMap<String, T>> held = Concurrent.newMap();
            ConcurrentMap<String, ConcurrentMap<String, T>> routed = Concurrent.newMap();

            for (String path : paths) {
                held.put(path, this.rowsIn(type, this.origin.read(path)));
                routed.put(path, Concurrent.newLinkedMap());
            }

            for (Map.Entry<String, T> entry : JpaModel.keyed(type, request.rows()).entrySet()) {
                String key = entry.getKey();

                if (delete) {
                    for (String path : paths) {
                        if (held.get(path).containsKey(key))
                            routed.get(path).put(key, entry.getValue());
                    }
                } else {
                    String owner = paths.getLast();

                    for (String path : paths) {
                        if (held.get(path).containsKey(key))
                            owner = path;
                    }

                    routed.get(owner).put(key, entry.getValue());
                }
            }

            Type listType = TypeToken.getParameterized(ConcurrentList.class, type).getType();

            for (String path : paths) {
                ConcurrentMap<String, T> rows = routed.get(path);

                if (rows.isEmpty())
                    continue;

                this.writes.edit(path, text -> {
                    ConcurrentMap<String, T> current = this.rowsIn(type, text);

                    for (Map.Entry<String, T> row : rows.entrySet()) {
                        if (delete)
                            current.remove(row.getKey());
                        else
                            current.put(row.getKey(), row.getValue());
                    }

                    return this.gson.toJson(Concurrent.newUnmodifiableList(current.values()), listType);
                });
            }
        }

    }

}

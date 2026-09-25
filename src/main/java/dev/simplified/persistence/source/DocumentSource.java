package dev.simplified.persistence.source;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.annotations.BuilderNames;
import dev.simplified.annotations.ClassBuilder;
import dev.simplified.annotations.SetterNames;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A source reading each type out of the layers of a tree of files - a GitHub repository, a directory
 * on disk, a bucket - that it is given as functions: which paths a logical name is made of, what the
 * text at a path is, and which documents moved.
 *
 * <p>A type names its document through the table name it already declares, the tree names that
 * document's layers, and the layers merge by key with the later one winning. That is one mechanism
 * for a generated file, its companion overrides and a local overlay, rather than three. A type's
 * fingerprint is its document's, which the tree answers when it can.
 *
 * <p>Reading is all a {@link ReadOnly} source promises, and it is no {@link Source.Writable}, so one
 * handed where a write is expected does not compile. A {@link ReadWrite} source is also given what the
 * text at a path becomes. Both are built the same way up to that instruction, and only the writable
 * builder takes it.
 */
@ClassBuilder(
    setters = @SetterNames(set = "with{}"),
    builder = @BuilderNames(from = BuilderNames.NONE, toBuilder = BuilderNames.NONE)
)
public abstract sealed class DocumentSource implements Source permits DocumentSource.ReadOnly, DocumentSource.ReadWrite {

    /**
     * The ordered paths a logical document is made of, empty when the tree publishes no such
     * document.
     *
     * <p>Order is merge order: a later path repeating a key replaces the row an earlier one carried,
     * which is what makes a companion file an override of the file it accompanies rather than a
     * second copy of it.
     */
    private final @NotNull Function<String, ConcurrentList<String>> layers;

    /**
     * The text at a path the layers name, relative to the tree's root.
     */
    private final @NotNull Function<String, String> text;

    /**
     * The fingerprint of every document the tree publishes as it stands now, keyed by logical
     * document name, empty when the tree cannot fingerprint.
     *
     * <p>A fingerprint covers every layer of its document, so a change to any of them moves it, and
     * two answers carrying the same fingerprint for a name describe the same text. A session asks
     * before it reads and skips a document whose fingerprint has not moved since, and a document the
     * answer leaves out is read as though it moved.
     */
    private final @NotNull Supplier<ConcurrentMap<String, String>> fingerprints = Concurrent::newUnmodifiableMap;

    /**
     * The instance documents are parsed and serialized with, which decides how a document's text
     * reads.
     */
    private final @NotNull Gson gson;

    /**
     * Constructs a source over the tree a builder collected.
     *
     * @param builder the builder holding the tree's functions and the parser
     * @throws JpaException if no layers, no text or no parser is given
     */
    protected DocumentSource(@NotNull Builder<?, ?> builder) {
        if (builder.layers == null)
            throw new JpaException("A document source names no layers");

        if (builder.text == null)
            throw new JpaException("A document source reads no text");

        if (builder.gson == null)
            throw new JpaException("A document source names no parser");

        this.layers = builder.layers;
        this.text = builder.text;
        this.fingerprints = builder.fingerprints;
        this.gson = builder.gson;
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

        for (String path : this.layersOf(type))
            merged.putAll(this.rowsIn(type, this.textAt(path)));

        return Concurrent.newUnmodifiableList(merged.values());
    }

    /**
     * {@inheritDoc}
     *
     * <p>The tree is asked once, and each type answers the fingerprint of the document its table
     * names. A type whose document the tree does not fingerprint is left out.
     */
    @Override
    public @NotNull ConcurrentMap<Class<? extends JpaModel>, String> fingerprints(
        @NotNull ConcurrentList<Class<JpaModel>> types
    ) throws JpaException {
        ConcurrentMap<String, String> documents = this.fingerprints.get();
        ConcurrentMap<Class<? extends JpaModel>, String> answered = Concurrent.newMap();

        if (documents.isEmpty())
            return answered;

        for (Class<JpaModel> type : types) {
            String fingerprint = documents.get(JpaModel.documentOf(type));

            if (fingerprint != null)
                answered.put(type, fingerprint);
        }

        return answered;
    }

    /**
     * The paths a type's document is made of, in merge order.
     *
     * @param type the entity class
     * @return the paths, never empty
     * @throws JpaException if the tree publishes no document under the type's name
     */
    final @NotNull ConcurrentList<String> layersOf(@NotNull Class<? extends JpaModel> type) throws JpaException {
        String name = JpaModel.documentOf(type);
        ConcurrentList<String> paths = this.layers.apply(name);

        if (paths.isEmpty())
            throw new JpaException("The source names no document '%s' for '%s'", name, type.getName());

        return paths;
    }

    /**
     * Reads the text at one path the layers name.
     *
     * @param path the path, relative to the tree's root
     * @return the text
     * @throws JpaException if the path cannot be read
     */
    final @NotNull String textAt(@NotNull String path) throws JpaException {
        return this.text.apply(path);
    }

    /**
     * Parses one layer of a type's document and keys its rows.
     *
     * @param type the entity class
     * @param body the layer's text
     * @param <T> the entity type
     * @return the layer's rows keyed by their id, in the layer's order
     */
    final <T extends JpaModel> @NotNull ConcurrentMap<String, T> rowsIn(@NotNull Class<T> type, @NotNull String body) {
        ConcurrentList<T> rows = this.gson.fromJson(body, listOf(type));
        return rows == null ? Concurrent.newLinkedMap() : JpaModel.keyed(type, rows);
    }

    /**
     * Serializes a layer's rows back into its text.
     *
     * @param type the entity class
     * @param rows the layer's rows, in the order they are written
     * @param <T> the entity type
     * @return the layer's text
     */
    final <T extends JpaModel> @NotNull String bodyOf(@NotNull Class<T> type, @NotNull ConcurrentMap<String, T> rows) {
        return this.gson.toJson(Concurrent.newUnmodifiableList(rows.values()), listOf(type));
    }

    /**
     * The list type a layer of a type's document parses as.
     *
     * @param type the entity class
     * @return the parameterized list type
     */
    private static @NotNull Type listOf(@NotNull Class<? extends JpaModel> type) {
        return TypeToken.getParameterized(ConcurrentList.class, type).getType();
    }

    /**
     * A document source that reads and never writes.
     */
    @ClassBuilder(
        setters = @SetterNames(set = "with{}"),
        builder = @BuilderNames(from = BuilderNames.NONE, toBuilder = BuilderNames.NONE)
    )
    public static final class ReadOnly extends DocumentSource {

        /**
         * Constructs a read-only source over the tree a builder collected.
         *
         * @param builder the builder holding the tree's functions and the parser
         * @throws JpaException if no layers, no text or no parser is given
         */
        ReadOnly(@NotNull Builder builder) {
            super(builder);
        }

    }

    /**
     * A document source over a tree a caller holds instructions to update.
     *
     * <p>The write instruction is the builder's to take, so only a builder that was given one builds a
     * {@link Source.Writable}. That is what keeps a caller holding no instruction from building a
     * source that claims one.
     */
    @ClassBuilder(
        setters = @SetterNames(set = "with{}"),
        builder = @BuilderNames(from = BuilderNames.NONE, toBuilder = BuilderNames.NONE)
    )
    public static final class ReadWrite extends DocumentSource implements Source.Writable {

        /**
         * The instruction replacing the text at one path, relative to the tree's root, with what a
         * change makes of it.
         *
         * <p>The tree reads the text together with whatever token it recognises for that revision - a
         * blob sha, a revision, a content hash - applies the change, and writes under that token. A
         * path that moves in between refuses the write, so a change is only ever applied to the text
         * it replaces. Editing a file this way reaches no session reading the tree: a registered type
         * is written through {@link JpaSession#write(WriteRequest)}, which rebuilds it.
         */
        private final @NotNull BiConsumer<String, UnaryOperator<String>> edit;

        /**
         * Constructs a writable source over the tree and the write instruction a builder collected.
         *
         * @param builder the builder holding the tree's functions, the parser and the write
         *        instruction
         * @throws JpaException if no layers, no text, no parser or no write instruction is given
         */
        ReadWrite(@NotNull Builder builder) {
            super(builder);

            if (builder.edit == null)
                throw new JpaException("A writable document source holds no write instruction");

            this.edit = builder.edit;
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
         * changed layer is then one {@linkplain #edit edit} of the tree, which applies only the rows
         * routed to that layer to its text as the tree holds it when it writes, so a row committed to
         * the layer in between survives beside the written one, and a layer that moves under the
         * tree's own read has the edit refused rather than written over it. Granularity is the tree's
         * problem rather than the caller's. Changed layers are edited in merge order, so a delete that
         * fails between two layers leaves the later layer's row, which is what a read answered before
         * the write, rather than an older one.
         */
        @Override
        public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
            if (request.rows().isEmpty())
                return;

            Class<T> type = request.type();
            boolean delete = request.operation() == WriteRequest.Operation.DELETE;
            ConcurrentList<String> paths = this.layersOf(type);
            ConcurrentMap<String, ConcurrentMap<String, T>> held = Concurrent.newMap();
            ConcurrentMap<String, ConcurrentMap<String, T>> routed = Concurrent.newMap();

            for (String path : paths) {
                held.put(path, this.rowsIn(type, this.textAt(path)));
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

            for (String path : paths) {
                ConcurrentMap<String, T> rows = routed.get(path);

                if (rows.isEmpty())
                    continue;

                this.edit.accept(path, body -> {
                    ConcurrentMap<String, T> current = this.rowsIn(type, body);

                    for (Map.Entry<String, T> row : rows.entrySet()) {
                        if (delete)
                            current.remove(row.getKey());
                        else
                            current.put(row.getKey(), row.getValue());
                    }

                    return this.bodyOf(type, current);
                });
            }
        }

    }

}

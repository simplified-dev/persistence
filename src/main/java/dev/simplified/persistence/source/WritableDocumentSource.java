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
 * A document source over an origin a caller holds instructions to update.
 *
 * <p>The write instruction is the origin's, so it is the origin's type that carries it: this takes a
 * {@link DocumentOrigin.Writable}, and handing it a read-only origin does not compile. That is what
 * keeps a caller holding no instruction from building a source that claims one.
 */
public final class WritableDocumentSource extends DocumentSource implements Source.Writable {

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
    public WritableDocumentSource(@NotNull DocumentOrigin.Writable origin, @NotNull Gson gson) {
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

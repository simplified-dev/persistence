package dev.simplified.persistence.store;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A remote origin's catalogue: the revision it was taken at, and the ordered layers each logical
 * document is made of.
 *
 * <p>It names no Java class and no repository. A logical name is the document's own name, which is
 * also the file stem and the table name the model declares, so a consumer resolves a type to a
 * document without the catalogue having to know one exists. Any origin that can publish a revision
 * and a list of hashed paths is describable this way.
 *
 * <p>Layers are ordered and merged by key with the later one winning, so a generated file and the
 * companion that overrides rows in it are one document rather than two mechanisms.
 *
 * @see Source#documents
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ManifestIndex {

    /**
     * The origin revision this catalogue was taken at, which a poller compares to rule out the whole
     * corpus in one request.
     */
    private final @NotNull String revision;

    /**
     * The layers each logical document is made of, keyed by logical name, in the order they merge.
     */
    private final @NotNull ConcurrentMap<String, ConcurrentList<Layer>> documents;

    /**
     * The layers a logical document is made of.
     *
     * @param name the logical document name
     * @return the layers in merge order, empty when the catalogue carries no such document
     */
    public @NotNull ConcurrentList<Layer> layersOf(@NotNull String name) {
        ConcurrentList<Layer> layers = this.documents.get(name);
        return layers == null ? Concurrent.newUnmodifiableList() : layers;
    }

    /**
     * The composed hash of every layer a logical document is made of, in merge order.
     *
     * <p>Every layer contributes, so a change to any of them moves the fingerprint. A rule reading
     * only the first layer's hash cannot see a companion change, which is the blind spot that makes
     * an override look like nothing happened.
     *
     * <p>An absent document answers empty, which a caller reads as "cannot claim unchanged" rather
     * than as "unchanged".
     *
     * @param name the logical document name
     * @return the composed fingerprint, empty when the catalogue carries no such document
     */
    public @NotNull Optional<String> fingerprintOf(@NotNull String name) {
        ConcurrentList<Layer> layers = this.layersOf(name);

        return layers.isEmpty()
            ? Optional.empty()
            : Optional.of(layers.stream().map(Layer::sha256).collect(Collectors.joining(":")));
    }

    /**
     * Returns a catalogue holding nothing.
     *
     * @return an empty catalogue at no revision
     */
    public static @NotNull ManifestIndex empty() {
        return new ManifestIndex("", Concurrent.newUnmodifiableMap());
    }

    /**
     * One layer of a logical document.
     *
     * @param path the layer's path, relative to the origin's root
     * @param sha256 the lowercase hex SHA-256 of the layer's bytes
     */
    public record Layer(@NotNull String path, @NotNull String sha256) {}

}

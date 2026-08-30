package dev.simplified.persistence.store;

import com.google.gson.Gson;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.model.LayeredRow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers what a second document layer does to the first.
 *
 * <p>The corpus this design reads exercises only addition - the one companion file that exists holds
 * two rows and neither is in the file it accompanies - so a merge that silently stopped overriding
 * would pass every test and every eyeball against real data. These cases override.
 */
class DocumentLayerMergeTest {

    private static final @NotNull Gson GSON = GsonSettings.defaults().create();

    /**
     * A catalogue naming one document made of the given layers, and a fetcher answering their bodies.
     */
    private static @NotNull Source of(@NotNull String @NotNull ... bodies) {
        ConcurrentList<ManifestIndex.Layer> layers = Concurrent.newList();
        ConcurrentMap<String, String> byPath = Concurrent.newMap();

        for (int index = 0; index < bodies.length; index++) {
            String path = "layer-" + index + ".json";
            layers.add(new ManifestIndex.Layer(path, "sha-" + index));
            byPath.put(path, bodies[index]);
        }

        ManifestIndex manifest = GSON.fromJson(
            GSON.toJson(new Catalogue("rev", Concurrent.newMap(java.util.Map.of("layered", layers)))),
            ManifestIndex.class
        );

        FileFetcher fetcher = path -> Optional.ofNullable(byPath.get(path))
            .orElseThrow(() -> new JpaException("No layer at '%s'", path));

        return Source.documents(() -> manifest, fetcher, GSON);
    }

    /**
     * The wire shape, so the test builds a catalogue the same way a consumer parses one.
     */
    private record Catalogue(@NotNull String revision, @NotNull ConcurrentMap<String, ConcurrentList<ManifestIndex.Layer>> documents) {}

    @Test
    @DisplayName("a later layer replaces a row the earlier one already carried")
    void laterLayerOverridesByKey() {
        Source source = of(
            "[{\"id\":\"A\",\"name\":\"first\"},{\"id\":\"B\",\"name\":\"second\"}]",
            "[{\"id\":\"B\",\"name\":\"overridden\"}]"
        );

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.stream().map(LayeredRow::getName).toList(), contains("first", "overridden"));
    }

    @Test
    @DisplayName("an overriding row keeps the position the first layer gave it")
    void overrideKeepsItsPlace() {
        Source source = of(
            "[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"},{\"id\":\"C\",\"name\":\"c\"}]",
            "[{\"id\":\"A\",\"name\":\"a2\"}]"
        );

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.stream().map(LayeredRow::getId).toList(), contains("A", "B", "C"));
        assertThat(rows.getFirst().getName(), equalTo("a2"));
    }

    @Test
    @DisplayName("a later layer adds a row the earlier one did not carry")
    void laterLayerAdds() {
        Source source = of(
            "[{\"id\":\"A\",\"name\":\"a\"}]",
            "[{\"id\":\"B\",\"name\":\"b\"}]"
        );

        assertThat(source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(), contains("A", "B"));
    }

    @Test
    @DisplayName("three layers resolve left to right, so the last one wins")
    void lastLayerWins() {
        Source source = of(
            "[{\"id\":\"A\",\"name\":\"first\"}]",
            "[{\"id\":\"A\",\"name\":\"second\"}]",
            "[{\"id\":\"A\",\"name\":\"third\"}]"
        );

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.size(), is(1));
        assertThat(rows.getFirst().getName(), equalTo("third"));
    }

    @Test
    @DisplayName("an empty layer changes nothing")
    void emptyLayerIsSilence() {
        Source source = of("[{\"id\":\"A\",\"name\":\"a\"}]", "[]");

        assertThat(source.read(LayeredRow.class).stream().map(LayeredRow::getName).toList(), contains("a"));
    }

    @Test
    @DisplayName("a type the catalogue names no document for fails rather than reading empty")
    void unnamedDocumentFails() {
        ManifestIndex empty = ManifestIndex.empty();
        Source source = Source.documents(() -> empty, path -> "[]", GSON);

        JpaException thrown = assertThrows(JpaException.class, () -> source.read(LayeredRow.class));
        assertThat(thrown.getMessage().contains("layered"), is(true));
    }

    @Test
    @DisplayName("the fingerprint moves when any layer moves, not only the first")
    void fingerprintComposesEveryLayer() {
        ManifestIndex one = catalogue(new ManifestIndex.Layer("a.json", "aaa"), new ManifestIndex.Layer("b.json", "bbb"));
        ManifestIndex two = catalogue(new ManifestIndex.Layer("a.json", "aaa"), new ManifestIndex.Layer("b.json", "ccc"));

        assertThat(one.fingerprintOf("layered").orElseThrow(), equalTo("aaa:bbb"));
        assertThat(two.fingerprintOf("layered").orElseThrow(), equalTo("aaa:ccc"));
        assertThat(one.fingerprintOf("layered").equals(two.fingerprintOf("layered")), is(false));
    }

    @Test
    @DisplayName("an unnamed document has no fingerprint, which is not the same as an unchanged one")
    void absentDocumentHasNoFingerprint() {
        assertThat(ManifestIndex.empty().fingerprintOf("layered").isEmpty(), is(true));
        assertThat(ManifestIndex.empty().layersOf("layered").isEmpty(), is(true));
    }

    private static @NotNull ManifestIndex catalogue(@NotNull ManifestIndex.Layer @NotNull ... layers) {
        ConcurrentList<ManifestIndex.Layer> listed = Concurrent.newList();
        listed.addAll(java.util.List.of(layers));

        return GSON.fromJson(
            GSON.toJson(new Catalogue("rev", Concurrent.newMap(java.util.Map.of("layered", listed)))),
            ManifestIndex.class
        );
    }

}

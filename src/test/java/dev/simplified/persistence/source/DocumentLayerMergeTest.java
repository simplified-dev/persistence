package dev.simplified.persistence.source;

import com.google.gson.Gson;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.unmapped.LayeredRow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers what a second document layer does to the first, and what a write does to both.
 *
 * <p>The corpus this design reads exercises only addition - the one companion file that exists holds
 * two rows and neither is in the file it accompanies - so a merge that silently stopped overriding
 * would pass every test and every eyeball against real data. These cases override.
 */
class DocumentLayerMergeTest {

    private static final @NotNull Gson GSON = GsonSettings.defaults().create();

    /**
     * A document origin over a map, so a case names its layers as bodies and nothing parses a
     * catalogue to set one up.
     *
     * <p>A body a case puts in {@code landing} is a commit someone else makes after the source read
     * the layer: it replaces the layer at the next edit of that path, before the origin reads the
     * text the edit applies to.
     */
    private static final class Layers implements DocumentOrigin.Writable {

        private final @NotNull ConcurrentMap<String, String> bodies = Concurrent.newLinkedMap();
        private final @NotNull ConcurrentMap<String, String> landing = Concurrent.newMap();
        private final @NotNull ConcurrentList<String> written = Concurrent.newList();

        private Layers(@NotNull String @NotNull ... bodies) {
            for (int index = 0; index < bodies.length; index++)
                this.bodies.put("layer-" + index + ".json", bodies[index]);
        }

        @Override
        public @NotNull ConcurrentList<String> layersOf(@NotNull String name) {
            return name.equals("layered")
                ? Concurrent.newUnmodifiableList(this.bodies.keySet())
                : Concurrent.newUnmodifiableList();
        }

        @Override
        public @NotNull String read(@NotNull String path) {
            return Optional.ofNullable(this.bodies.get(path))
                .orElseThrow(() -> new JpaException("No layer at '%s'", path));
        }

        @Override
        public void edit(@NotNull String path, @NotNull UnaryOperator<String> change) {
            String landed = this.landing.remove(path);

            if (landed != null)
                this.bodies.put(path, landed);

            this.bodies.put(path, change.apply(this.read(path)));
            this.written.add(path);
        }

    }

    private static @NotNull Source of(@NotNull String @NotNull ... bodies) {
        return new DocumentSource(new Layers(bodies), GSON);
    }

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
    @DisplayName("a type the origin names no document for fails rather than reading empty")
    void unnamedDocumentFails() {
        Source source = of();

        JpaException thrown = assertThrows(JpaException.class, () -> source.read(LayeredRow.class));
        assertThat(thrown.getMessage().contains("layered"), is(true));
    }

    @Test
    @DisplayName("a new row lands in the last layer, and the first is not written")
    void newRowLandsInTheLastLayer() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"}]",
            "[{\"id\":\"B\",\"name\":\"b\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("C", "c"))));

        assertThat(origin.written, contains("layer-1.json"));
        assertThat(origin.bodies.get("layer-0.json"), equalTo("[{\"id\":\"A\",\"name\":\"a\"}]"));
        assertThat(idsIn(origin, "layer-1.json"), contains("B", "C"));
        assertThat(
            source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(),
            contains("A", "B", "C")
        );
    }

    @Test
    @DisplayName("an upsert of an overridden key lands in the override, so the read answers it")
    void overriddenUpsertLandsInTheOverride() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]",
            "[{\"id\":\"B\",\"name\":\"b1\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("B", "b2"))));

        assertThat(origin.written, contains("layer-1.json"));
        assertThat(
            origin.bodies.get("layer-0.json"),
            equalTo("[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]")
        );

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.stream().map(LayeredRow::getId).toList(), contains("A", "B"));
        assertThat(rows.getLast().getName(), equalTo("b2"));
    }

    @Test
    @DisplayName("a delete of an overridden key leaves no layer carrying it, writing both in merge order")
    void overriddenDeleteLeavesNoLayerCarryingIt() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]",
            "[{\"id\":\"B\",\"name\":\"b1\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.delete(LayeredRow.class, List.of(row("B", "b1"))));

        assertThat(origin.written, contains("layer-0.json", "layer-1.json"));
        assertThat(idsIn(origin, "layer-0.json"), contains("A"));
        assertThat(idsIn(origin, "layer-1.json"), is(empty()));
        assertThat(source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(), contains("A"));
    }

    @Test
    @DisplayName("a write naming only keys the first layer owns leaves the override unwritten")
    void firstLayerWriteLeavesTheOverrideUnwritten() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]",
            "[{\"id\":\"C\",\"name\":\"c\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("A", "a2"))));

        assertThat(origin.written, contains("layer-0.json"));
        assertThat(idsIn(origin, "layer-0.json"), contains("A", "B"));
        assertThat(origin.bodies.get("layer-1.json"), equalTo("[{\"id\":\"C\",\"name\":\"c\"}]"));

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.stream().map(LayeredRow::getId).toList(), contains("A", "B", "C"));
        assertThat(rows.getFirst().getName(), equalTo("a2"));
    }

    @Test
    @DisplayName("one upsert naming keys different layers own, and a new key, writes each owning layer once, in merge order")
    void mixedOwnerUpsertWritesEachOwner() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"}]",
            "[{\"id\":\"B\",\"name\":\"b\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("A", "a2"), row("B", "b2"), row("C", "c"))));

        assertThat(origin.written, contains("layer-0.json", "layer-1.json"));
        assertThat(idsIn(origin, "layer-0.json"), contains("A"));
        assertThat(idsIn(origin, "layer-1.json"), contains("B", "C"));
        assertThat(
            source.read(LayeredRow.class).stream().map(LayeredRow::getName).toList(),
            contains("a2", "b2", "c")
        );
    }

    @Test
    @DisplayName("a delete of a key no layer carries writes nothing")
    void deleteOfAnUncarriedKeyWritesNothing() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"}]",
            "[{\"id\":\"B\",\"name\":\"b\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.delete(LayeredRow.class, List.of(row("Z", "z"))));

        assertThat(origin.written, is(empty()));
    }

    @Test
    @DisplayName("a written row replaces the one already under its key rather than joining it")
    void writeOverridesByKey() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]");
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("A", "rewritten"))));

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.stream().map(LayeredRow::getId).toList(), contains("A", "B"));
        assertThat(rows.getFirst().getName(), equalTo("rewritten"));
    }

    @Test
    @DisplayName("a delete removes by key and leaves the rest of the document")
    void deleteRemovesByKey() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]",
            "[{\"id\":\"C\",\"name\":\"c\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.delete(LayeredRow.class, List.of(row("B", "b"))));

        assertThat(source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(), contains("A", "C"));
    }

    @Test
    @DisplayName("a write naming no rows does not reach the origin at all")
    void emptyWriteIsSilence() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"}]");
        Source.Writable source = new DocumentSource.Writable(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of()));

        assertThat(origin.written, is(empty()));
        assertThat(origin.bodies.get("layer-0.json"), equalTo("[{\"id\":\"A\",\"name\":\"a\"}]"));
    }

    @Test
    @DisplayName("a write applies to the layer as the origin holds it when it writes, so a row committed in between survives")
    void writeAppliesToTheLayerTheOriginHolds() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"}]",
            "[{\"id\":\"B\",\"name\":\"b\"}]"
        );
        Source.Writable source = new DocumentSource.Writable(origin, GSON);
        origin.landing.put("layer-1.json", "[{\"id\":\"B\",\"name\":\"b\"},{\"id\":\"D\",\"name\":\"d\"}]");

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("C", "c"))));

        assertThat(origin.written, contains("layer-1.json"));
        assertThat(idsIn(origin, "layer-1.json"), contains("B", "D", "C"));
        assertThat(
            source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(),
            contains("A", "B", "D", "C")
        );
    }

    @Test
    @DisplayName("a delete removes only the keys it names from the layer as the origin holds it when it writes")
    void deleteAppliesToTheLayerTheOriginHolds() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]");
        Source.Writable source = new DocumentSource.Writable(origin, GSON);
        origin.landing.put(
            "layer-0.json",
            "[{\"id\":\"A\",\"name\":\"a2\"},{\"id\":\"B\",\"name\":\"b\"},{\"id\":\"D\",\"name\":\"d\"}]"
        );

        source.write(WriteRequest.delete(LayeredRow.class, List.of(row("B", "b"))));

        ConcurrentList<LayeredRow> rows = source.read(LayeredRow.class);

        assertThat(rows.stream().map(LayeredRow::getId).toList(), contains("A", "D"));
        assertThat(rows.getFirst().getName(), equalTo("a2"));
    }

    private static @NotNull List<String> idsIn(@NotNull Layers origin, @NotNull String path) {
        return Arrays.stream(GSON.fromJson(origin.bodies.get(path), LayeredRow[].class))
            .map(LayeredRow::getId)
            .toList();
    }

    private static @NotNull LayeredRow row(@NotNull String id, @NotNull String name) {
        LayeredRow row = new LayeredRow();
        row.setId(id);
        row.setName(name);
        return row;
    }

}

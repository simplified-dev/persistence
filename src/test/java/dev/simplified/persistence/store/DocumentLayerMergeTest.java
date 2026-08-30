package dev.simplified.persistence.store;

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

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
     */
    private static final class Layers implements DocumentOrigin.Writable {

        private final @NotNull ConcurrentMap<String, String> bodies = Concurrent.newLinkedMap();
        private @NotNull Optional<String> precondition = Optional.empty();

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
        public void write(@NotNull String path, @NotNull String content, @NotNull Optional<String> precondition) {
            this.bodies.put(path, content);
            this.precondition = precondition;
        }

    }

    private static @NotNull Source of(@NotNull String @NotNull ... bodies) {
        return Source.documents(new Layers(bodies), GSON);
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
    @DisplayName("a write rewrites the first layer carrying every layer's rows")
    void writeCarriesTheWholeDocument() {
        Layers origin = new Layers(
            "[{\"id\":\"A\",\"name\":\"a\"}]",
            "[{\"id\":\"B\",\"name\":\"b\"}]"
        );
        Source.Writable source = Source.documents(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("C", "c"))));

        // The second layer is untouched, so a re-read would see B twice if the first layer had been
        // rewritten with only what the request named.
        assertThat(origin.bodies.get("layer-1.json"), equalTo("[{\"id\":\"B\",\"name\":\"b\"}]"));
        assertThat(
            source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(),
            contains("A", "B", "C")
        );
    }

    @Test
    @DisplayName("a written row replaces the one already under its key rather than joining it")
    void writeOverridesByKey() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"},{\"id\":\"B\",\"name\":\"b\"}]");
        Source.Writable source = Source.documents(origin, GSON);

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
        Source.Writable source = Source.documents(origin, GSON);

        source.write(WriteRequest.delete(LayeredRow.class, List.of(row("B", "b"))));

        assertThat(source.read(LayeredRow.class).stream().map(LayeredRow::getId).toList(), contains("A", "C"));
    }

    @Test
    @DisplayName("a write naming no rows reaches the origin at all")
    void emptyWriteIsSilence() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"}]");
        Source.Writable source = Source.documents(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of()));

        assertThat(origin.bodies.get("layer-0.json"), equalTo("[{\"id\":\"A\",\"name\":\"a\"}]"));
    }

    @Test
    @DisplayName("the precondition the request names is the one the origin is handed")
    void preconditionReachesTheOrigin() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"}]");
        Source.Writable source = Source.documents(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("B", "b"))).expecting("blob-sha"));

        assertThat(origin.precondition, equalTo(Optional.of("blob-sha")));
    }

    @Test
    @DisplayName("a request naming no precondition leaves the origin to resolve its own")
    void unconditionalWriteNamesNoPrecondition() {
        Layers origin = new Layers("[{\"id\":\"A\",\"name\":\"a\"}]");
        Source.Writable source = Source.documents(origin, GSON);

        source.write(WriteRequest.upsert(LayeredRow.class, List.of(row("B", "b"))));

        assertThat(origin.precondition.isEmpty(), is(true));
    }

    private static @NotNull LayeredRow row(@NotNull String id, @NotNull String name) {
        LayeredRow row = new LayeredRow();
        row.setId(id);
        row.setName(name);
        return row;
    }

}

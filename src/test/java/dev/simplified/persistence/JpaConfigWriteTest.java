package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.linked.LinkedChild;
import dev.simplified.persistence.linked.LinkedCorpus;
import dev.simplified.persistence.linked.LinkedGrandchild;
import dev.simplified.persistence.linked.LinkedParent;
import dev.simplified.persistence.optional.LinkedStray;
import dev.simplified.persistence.sibling.LinkedSibling;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static dev.simplified.persistence.linked.LinkedCorpus.child;
import static dev.simplified.persistence.linked.LinkedCorpus.parent;
import static dev.simplified.persistence.linked.LinkedCorpus.sibling;
import static dev.simplified.persistence.linked.LinkedCorpus.stray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The checked write a caller holding no session makes through {@link JpaConfig#write(WriteRequest)}:
 * a write that would leave a plain link naming no row never reaches the source, whether it upserts a
 * row naming a missing one or deletes a row still named, while a link that tolerates a miss refuses
 * nothing. The check reads only the types a plain link needs, each at most once, and the write
 * rebuilds nothing.
 */
class JpaConfigWriteTest {

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    private LinkedCorpus corpus;
    private JpaConfig config;

    @BeforeEach
    void seed() {
        this.corpus = new LinkedCorpus();
        this.corpus.parents.put("p1", "one");
        this.corpus.parents.put("p2", "two");
        this.corpus.children.put("c1", "p1");
        this.corpus.grandchildren.put("g1", "c1");
        this.corpus.strays.put("s1", Optional.of("p2"));

        this.config = new JpaConfig(
            models(LinkedParent.class, LinkedChild.class, LinkedGrandchild.class, LinkedStray.class, LinkedSibling.class),
            this.corpus
        );
    }

    @Test
    @DisplayName("an upsert with a row whose plain link names no row is refused whole and never reaches the source")
    void anUpsertNamingAMissingRowNeverReachesTheSource() {
        JpaException thrown = assertThrows(
            JpaException.class,
            () -> this.config.write(WriteRequest.upsert(LinkedChild.class, List.of(child("c2", "p1"), child("c3", "p9"))))
        );

        assertThat(thrown.getMessage(), equalTo("Field 'parent' of '" + LinkedChild.class.getName() + "' names 'p9', which no row carries"));
        assertThat(this.corpus.writes(), equalTo(0));
        assertThat(this.corpus.children.keySet(), contains("c1"));
    }

    @Test
    @DisplayName("an upsert whose rows name each other lands, answered by its own rows without a read")
    void anUpsertNamingARowOfItsOwnRequestLands() {
        this.config.write(WriteRequest.upsert(LinkedSibling.class, List.of(sibling("s2", "s3"), sibling("s3", "s2"))));

        assertThat(this.corpus.writes(), equalTo(1));
        assertThat(this.corpus.siblings.keySet(), containsInAnyOrder("s2", "s3"));
        assertThat(this.corpus.readsOf(LinkedSibling.class), equalTo(0));
    }

    @Test
    @DisplayName("a plain link into a type no registered type answers for is refused before anything is read")
    void aLinkIntoAnUnregisteredTypeIsRefused() {
        JpaConfig children = new JpaConfig(models(LinkedChild.class), this.corpus);

        JpaException thrown = assertThrows(
            JpaException.class,
            () -> children.write(WriteRequest.upsert(LinkedChild.class, List.of(child("c2", "p1"))))
        );

        assertThat(thrown.getMessage(), equalTo(
            "Field 'parent' of '" + LinkedChild.class.getName() + "' links into '" + LinkedParent.class.getName() + "', which no registered type answers for"
        ));
        assertThat(this.corpus.writes(), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(0));
    }

    @Test
    @DisplayName("a delete of a row a plain link still names is refused whole and never reaches the source")
    void aDeleteOfARowAPlainLinkNamesNeverReachesTheSource() {
        JpaException thrown = assertThrows(
            JpaException.class,
            () -> this.config.write(WriteRequest.delete(LinkedParent.class, List.of(parent("p1", "one"))))
        );

        assertThat(thrown.getMessage(), equalTo("Field 'parent' of '" + LinkedChild.class.getName() + "' names 'p1', which the write deletes"));
        assertThat(this.corpus.writes(), equalTo(0));
        assertThat(this.corpus.parents.keySet(), contains("p1", "p2"));
    }

    @Test
    @DisplayName("a delete of a row only an Optional link names lands")
    void aDeleteNamedOnlyByAnOptionalLands() {
        this.config.write(WriteRequest.delete(LinkedParent.class, List.of(parent("p2", "two"))));

        assertThat(this.corpus.writes(), equalTo(1));
        assertThat(this.corpus.parents.keySet(), contains("p1"));
        assertThat(this.corpus.strays.get("s1"), equalTo(Optional.of("p2")));
    }

    @Test
    @DisplayName("the check reads only the types a plain link needs, each at most once per write")
    void aCheckReadsOnlyPlainLinkTargets() {
        this.config.write(WriteRequest.upsert(LinkedChild.class, List.of(child("c2", "p1"), child("c3", "p1"))));

        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(1));
        assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(0));

        this.config.write(WriteRequest.upsert(LinkedStray.class, List.of(stray("s2", "p9"))));

        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(1));
        assertThat(this.corpus.readsOf(LinkedStray.class), equalTo(0));

        this.config.write(WriteRequest.delete(LinkedParent.class, List.of(parent("p2", "two"))));

        assertThat(this.corpus.writes(), equalTo(3));
        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(1));
        assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(1));
        assertThat(this.corpus.readsOf(LinkedGrandchild.class), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedStray.class), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedSibling.class), equalTo(0));
    }

    @Test
    @DisplayName("a write rebuilds nothing and reaches the source exactly once")
    void aWriteRebuildsNothing() {
        this.config.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p1", "uno"))));

        assertThat(this.corpus.writes(), equalTo(1));
        assertThat(this.corpus.parents.get("p1"), equalTo("uno"));
        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedChild.class), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedGrandchild.class), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedStray.class), equalTo(0));
    }

    @Test
    @DisplayName("a request naming no rows writes nothing and reads nothing")
    void anEmptyRequestWritesNothing() {
        this.config.write(WriteRequest.upsert(LinkedChild.class, List.of()));

        assertThat(this.corpus.writes(), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(0));
    }

    @Test
    @DisplayName("a type not registered exactly, or a source that takes no write, is refused before anything is read")
    void anUnregisteredTypeOrAReadOnlySourceIsRefused() {
        JpaConfig parents = new JpaConfig(models(LinkedParent.class), this.corpus);
        JpaConfig readOnly = new JpaConfig(models(LinkedParent.class), new Source() {

            @Override
            public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
                return JpaConfigWriteTest.this.corpus.read(type);
            }

        });

        JpaException unregistered = assertThrows(
            JpaException.class,
            () -> parents.write(WriteRequest.upsert(LinkedChild.class, List.of(child("c2", "p1"))))
        );
        JpaException unwritable = assertThrows(
            JpaException.class,
            () -> readOnly.write(WriteRequest.upsert(LinkedParent.class, List.of(parent("p3", "three"))))
        );

        assertThat(unregistered.getMessage(), equalTo("Config registers no '" + LinkedChild.class.getName() + "' to write it"));
        assertThat(unwritable.getMessage(), equalTo("Source for '" + LinkedParent.class.getName() + "' holds no write instruction"));
        assertThat(this.corpus.writes(), equalTo(0));
        assertThat(this.corpus.readsOf(LinkedParent.class), equalTo(0));
    }

}

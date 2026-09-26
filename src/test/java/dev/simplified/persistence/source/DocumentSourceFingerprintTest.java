package dev.simplified.persistence.source;

import com.google.gson.Gson;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.model.TestChildModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.unmapped.ContractRow;
import dev.simplified.persistence.unmapped.LayeredRow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * What a document source answers when a session asks for fingerprints: each type the fingerprint of
 * the document its table names, nothing for a type whose document the tree does not fingerprint, and
 * nothing at all from a tree that cannot fingerprint.
 *
 * <p>Every source here fails a layer lookup or a read, so each case also shows that asking for
 * fingerprints reads no document.
 */
class DocumentSourceFingerprintTest {

    private static final @NotNull Gson GSON = GsonSettings.defaults().create();

    /**
     * Starts a read-only source whose every layer lookup and read fails the case, fingerprinting
     * nothing until the case gives it fingerprints.
     *
     * @return the builder
     */
    private static @NotNull DocumentSource.ReadOnly.Builder unreadable() {
        return DocumentSource.ReadOnly.builder()
            .withLayers(name -> { throw new AssertionError(String.format("Asking for fingerprints looked up the layers of '%s'", name)); })
            .withText(path -> { throw new AssertionError(String.format("Asking for fingerprints read '%s'", path)); })
            .withGson(GSON);
    }

    /**
     * Builds an unreadable source whose tree answers a fixed fingerprint per document name.
     *
     * @param documents the fingerprints, keyed by document name
     * @return the source
     */
    private static @NotNull Source fingerprinted(@NotNull Map<String, String> documents) {
        ConcurrentMap<String, String> answered = Concurrent.newMap();
        answered.putAll(documents);

        return unreadable().withFingerprints(() -> answered).build();
    }

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    @Test
    @DisplayName("each type answers the fingerprint of the document its table names")
    void eachTypeAnswersItsDocumentsFingerprint() {
        Source source = fingerprinted(Map.of(
            "layered", "layered-one",
            "test_parent", "parent-one",
            "unregistered", "unregistered-one"
        ));

        ConcurrentMap<Class<? extends JpaModel>, String> answered = source.fingerprints(models(LayeredRow.class, TestParentModel.class));

        assertThat(answered, aMapWithSize(2));
        assertThat(answered, hasEntry(LayeredRow.class, "layered-one"));
        assertThat(answered, hasEntry(TestParentModel.class, "parent-one"));
    }

    @Test
    @DisplayName("a type whose document the tree does not fingerprint is left out")
    void anUnfingerprintedDocumentsTypeIsLeftOut() {
        Source source = fingerprinted(Map.of("layered", "layered-one"));

        ConcurrentMap<Class<? extends JpaModel>, String> answered = source.fingerprints(models(LayeredRow.class, TestChildModel.class));

        assertThat(answered, not(hasKey(TestChildModel.class)));
        assertThat(answered.get(LayeredRow.class), equalTo("layered-one"));
        assertThat(answered, aMapWithSize(1));
    }

    @Test
    @DisplayName("a tree that cannot fingerprint leaves every type out, without naming any type's document")
    void aTreeThatCannotFingerprintAnswersNothing() {
        Source source = unreadable().build();

        // ContractRow declares no table, so resolving its document would throw.
        ConcurrentMap<Class<? extends JpaModel>, String> answered = source.fingerprints(models(LayeredRow.class, ContractRow.class));

        assertThat(answered, anEmptyMap());
    }

}

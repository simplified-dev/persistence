package dev.simplified.persistence.source;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.model.TestParentModel;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link WriteRequest} covering the static factories, field
 * population, JDK {@link Serializable} round-trip (which exercises the
 * exact serializer path Hazelcast will use on the {@code skyblock.writes} queue),
 * and the {@link WriteRequest#resolveEntityType()} /
 * {@link WriteRequest#deserializeEntity(Gson, Class)} helpers used by the Phase 6b
 * consumer.
 *
 * <p>Uses {@link TestParentModel} as the concrete entity fixture because it is
 * already a real {@link JpaModel} in the persistence test sources and can round
 * trip through Gson with no additional type-adapter plumbing.
 */
class WriteRequestTest {

    private static final Gson GSON = new Gson();
    private static final String SOURCE_ID = "skyblock-data";

    @Test
    @DisplayName("upsert factory populates every field with the expected values")
    void upsertFactoryPopulatesAllFields() {
        Instant before = Instant.now();

        TestParentModel entity = new TestParentModel();
        entity.setId(1);
        entity.setName("parent1");

        WriteRequest request = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);

        Instant after = Instant.now();

        assertThat(request.getRequestId(), is(notNullValue()));
        assertThat(request.getTimestamp(), is(notNullValue()));
        assertThat(request.getTimestamp(), greaterThanOrEqualTo(before));
        assertThat(request.getTimestamp(), lessThanOrEqualTo(after));
        assertThat(request.getOperation(), equalTo(WriteRequest.Operation.UPSERT));
        assertThat(request.getEntityClassName(), equalTo(TestParentModel.class.getName()));
        assertThat(request.getEntityJson(), containsString("\"id\":1"));
        assertThat(request.getEntityJson(), containsString("\"name\":\"parent1\""));
        assertThat(request.getSourceId(), equalTo(SOURCE_ID));
    }

    @Test
    @DisplayName("delete factory populates every field with DELETE operation")
    void deleteFactoryPopulatesAllFields() {
        TestParentModel entity = new TestParentModel();
        entity.setId(2);
        entity.setName("parent2");

        WriteRequest request = WriteRequest.delete(TestParentModel.class, entity, GSON, SOURCE_ID);

        assertThat(request.getOperation(), equalTo(WriteRequest.Operation.DELETE));
        assertThat(request.getEntityClassName(), equalTo(TestParentModel.class.getName()));
        assertThat(request.getEntityJson(), containsString("\"id\":2"));
        assertThat(request.getSourceId(), equalTo(SOURCE_ID));
    }

    @Test
    @DisplayName("consecutive factory calls produce different requestIds")
    void consecutiveCallsProduceDifferentRequestIds() {
        TestParentModel entity = new TestParentModel();
        entity.setId(1);
        entity.setName("parent1");

        WriteRequest first = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);
        WriteRequest second = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);

        assertThat(first.getRequestId(), not(equalTo(second.getRequestId())));
    }

    @Test
    @DisplayName("JDK Serializable round-trip preserves every field")
    void jdkSerializableRoundTripPreservesFields() throws Exception {
        TestParentModel entity = new TestParentModel();
        entity.setId(42);
        entity.setName("round-trip");

        WriteRequest original = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);

        // Use plain ObjectOutputStream/ObjectInputStream to mirror the exact path
        // Hazelcast's JavaDefaultSerializers$JavaSerializer takes for Serializable
        // payloads that lack a custom registration.
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
            oos.writeObject(original);
        }

        WriteRequest restored;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            restored = (WriteRequest) ois.readObject();
        }

        assertThat(restored, is(notNullValue()));
        assertThat(restored.getRequestId(), equalTo(original.getRequestId()));
        assertThat(restored.getTimestamp(), equalTo(original.getTimestamp()));
        assertThat(restored.getOperation(), equalTo(original.getOperation()));
        assertThat(restored.getEntityClassName(), equalTo(original.getEntityClassName()));
        assertThat(restored.getEntityJson(), equalTo(original.getEntityJson()));
        assertThat(restored.getSourceId(), equalTo(original.getSourceId()));
    }

    @Test
    @DisplayName("withRequestId replaces the request id and preserves every other field")
    void withRequestIdReplacesIdAndPreservesOtherFields() {
        TestParentModel entity = new TestParentModel();
        entity.setId(5);
        entity.setName("retry-me");

        WriteRequest original = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);
        UUID replacement = UUID.randomUUID();

        WriteRequest swapped = original.withRequestId(replacement);

        // New instance carries the replacement id with every other field copied byte-identically.
        assertThat(swapped, is(notNullValue()));
        assertThat(swapped.getRequestId(), equalTo(replacement));
        assertThat(swapped.getTimestamp(), equalTo(original.getTimestamp()));
        assertThat(swapped.getOperation(), equalTo(original.getOperation()));
        assertThat(swapped.getEntityClassName(), equalTo(original.getEntityClassName()));
        assertThat(swapped.getEntityJson(), equalTo(original.getEntityJson()));
        assertThat(swapped.getSourceId(), equalTo(original.getSourceId()));

        // Source instance is unchanged (immutability check).
        assertThat(original.getRequestId(), not(equalTo(replacement)));
    }

    @Test
    @DisplayName("resolveEntityType returns the original class for a valid JpaModel FQCN")
    void resolveEntityTypeReturnsClassForValidFqcn() {
        TestParentModel entity = new TestParentModel();
        entity.setId(1);
        entity.setName("parent1");

        WriteRequest request = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);

        Class<? extends JpaModel> resolved = assertDoesNotThrow(request::resolveEntityType);
        assertThat(resolved, equalTo(TestParentModel.class));
    }

    @Test
    @DisplayName("resolveEntityType wraps ClassNotFoundException in JpaException with the FQCN in the message")
    void resolveEntityTypeWrapsClassNotFoundException() {
        // Reflection-construct a request with a deliberately invalid FQCN so we
        // exercise the consumer's defensive path for stale manifest references.
        WriteRequest bogus = buildWithClassName("com.example.DoesNotExist");

        JpaException thrown = assertThrows(JpaException.class, bogus::resolveEntityType);
        assertThat(thrown.getMessage(), containsString("com.example.DoesNotExist"));
        assertThat(thrown.getCause(), is(notNullValue()));
    }

    @Test
    @DisplayName("resolveEntityType rejects a class that does not implement JpaModel")
    void resolveEntityTypeRejectsNonJpaModel() {
        WriteRequest bogus = buildWithClassName(String.class.getName());

        JpaException thrown = assertThrows(JpaException.class, bogus::resolveEntityType);
        assertThat(thrown.getMessage(), containsString("does not implement JpaModel"));
    }

    @Test
    @DisplayName("deserializeEntity rehydrates the producer's entity state from the stored JSON")
    void deserializeEntityRehydratesFromJson() {
        TestParentModel original = new TestParentModel();
        original.setId(7);
        original.setName("rehydrated");

        WriteRequest request = WriteRequest.upsert(TestParentModel.class, original, GSON, SOURCE_ID);

        TestParentModel rehydrated = request.deserializeEntity(GSON, TestParentModel.class);

        assertThat(rehydrated, is(notNullValue()));
        assertThat(rehydrated.getId(), equalTo(7));
        assertThat(rehydrated.getName(), equalTo("rehydrated"));
    }

    @Test
    @DisplayName("deserializeEntity wraps malformed JSON in JpaException")
    void deserializeEntityWrapsMalformedJson() {
        WriteRequest bogus = buildWithEntityJson("{not valid json");

        JpaException thrown = assertThrows(
            JpaException.class,
            () -> bogus.deserializeEntity(GSON, TestParentModel.class)
        );
        assertThat(thrown.getMessage(), containsString(TestParentModel.class.getName()));
    }

    // --- helpers --- //

    /**
     * Constructs a {@link WriteRequest} with an arbitrary {@code entityClassName}
     * via JDK serialization (since the private constructor is not directly
     * accessible). Builds a real {@code WriteRequest}, serializes it, then swaps
     * the target-field byte sequence via a round-trip through reflection on the
     * known frozen field names.
     *
     * <p>Uses reflection to set final fields because the alternative - adding a
     * test-only package-private constructor - leaks test concerns into the
     * production class. The reflection path is confined to these helpers and only
     * runs in the test JVM.
     */
    private static @NotNull WriteRequest buildWithClassName(@NotNull String className) {
        return overrideField("entityClassName", className);
    }

    private static @NotNull WriteRequest buildWithEntityJson(@NotNull String entityJson) {
        return overrideField("entityJson", entityJson);
    }

    @SuppressWarnings("SameParameterValue")
    private static @NotNull WriteRequest overrideField(@NotNull String fieldName, @NotNull String value) {
        TestParentModel entity = new TestParentModel();
        entity.setId(1);
        entity.setName("placeholder");

        WriteRequest request = WriteRequest.upsert(TestParentModel.class, entity, GSON, SOURCE_ID);

        try {
            java.lang.reflect.Field field = WriteRequest.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(request, value);
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError("reflection helper failed", ex);
        }

        return request;
    }

}

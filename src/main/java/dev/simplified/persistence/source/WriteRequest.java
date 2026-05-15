package dev.simplified.persistence.source;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Envelope describing a single write request against a {@link MutableSource}.
 *
 * <p>Phase 6 wire format for the {@code IQueue<WriteRequest>} write path. Producers
 * ({@code simplified-bot} and any future mutators) construct a {@code WriteRequest}
 * via {@link #upsert(Class, JpaModel, Gson, String)} or
 * {@link #delete(Class, JpaModel, Gson, String)}, push it onto the Hazelcast
 * {@code skyblock.writes} queue, and immediately return. The single consumer
 * ({@code simplified-data}) drains the queue, resolves the target
 * {@link MutableSource} via {@link #getSourceId()}, and applies the mutation.
 *
 * <p>The envelope is deliberately plain Java {@link Serializable} with no
 * Hazelcast-specific interfaces like {@code DataSerializable} or {@code Portable}.
 * Hazelcast's {@code AbstractSerializationService} falls back to the built-in
 * {@code JavaDefaultSerializers$JavaSerializer} for any type that implements only
 * {@code Serializable}, which walks the object graph via a standard
 * {@link ObjectOutputStream}. Every field on this class is a primitive,
 * enum, or JDK value type ({@link String}, {@link UUID}, {@link Instant}) that
 * serializes cleanly through the default path, so no custom serializer is needed.
 *
 * <p>The {@link #entityJson} payload is pre-serialized by the producer via the
 * producer's local {@link Gson} instance. This keeps the library free of Gson
 * configuration coupling and lets each service ship its own
 * {@link GsonSettings} without cross-service drift. The
 * consumer rehydrates the entity via {@link #deserializeEntity(Gson, Class)} using
 * its own Gson, which is assumed to produce a byte-compatible deserialization for
 * the shared model class (the {@code minecraft-api} dep guarantees both sides see
 * the same {@link JpaModel} classes and Gson type adapters).
 *
 * <p>Instances are constructed exclusively via the static factories; the
 * constructor is {@code private} under Lombok's {@code @RequiredArgsConstructor},
 * matching the rest of the package's convention (see {@link ManifestIndex}). All
 * fields are final and non-null.
 *
 * @see MutableSource
 * @see Operation
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class WriteRequest implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * UUID identifying this specific write request. Producers generate a fresh UUID
     * per call via {@link UUID#randomUUID()}; consumers surface the id in audit logs
     * and use it for future dedup if the write path ever moves to at-least-once
     * semantics with idempotent replay.
     */
    private final @NotNull UUID requestId;

    /**
     * Instant at which the producer constructed this request, captured via
     * {@link Instant#now()}. The consumer's batch scheduler uses this for TTL
     * decisions if a write sits in the queue longer than the batch window.
     */
    private final @NotNull Instant timestamp;

    /**
     * Whether this request represents an upsert or a delete.
     */
    private final @NotNull Operation operation;

    /**
     * Fully qualified name of the target {@link JpaModel} class, captured from
     * {@link Class#getName()} on the producer side. The consumer resolves this back
     * to a live class via {@link #resolveEntityType()} when it is ready to dispatch
     * the request to a {@link MutableSource}.
     */
    private final @NotNull String entityClassName;

    /**
     * JSON body of the entity as serialized by the producer's Gson instance. Stored
     * as a raw string rather than a byte array because the consumer's Gson
     * {@code fromJson} entry points accept {@link String} directly and there is no
     * benefit to holding the UTF-8 bytes.
     */
    private final @NotNull String entityJson;

    /**
     * Human-readable source id identifying which {@link MutableSource} should apply
     * this write. Matches the {@code sourceId} used by the Phase 4a
     * {@link RemoteJsonSource} chain and the Phase 4c {@code ExternalAssetState} row
     * (for example {@code "skyblock-data"}).
     */
    private final @NotNull String sourceId;

    /**
     * Builds an upsert request for the given entity.
     *
     * @param entityType the entity class, used only to capture the FQCN
     * @param entity the entity to persist
     * @param gson the producer's Gson instance used to serialize {@code entity}
     * @param sourceId the target {@link MutableSource} id
     * @param <T> the entity type
     * @return a new write request with {@link Operation#UPSERT}
     */
    public static <T extends JpaModel> @NotNull WriteRequest upsert(
        @NotNull Class<T> entityType,
        @NotNull T entity,
        @NotNull Gson gson,
        @NotNull String sourceId
    ) {
        return new WriteRequest(
            UUID.randomUUID(),
            Instant.now(),
            Operation.UPSERT,
            entityType.getName(),
            gson.toJson(entity),
            sourceId
        );
    }

    /**
     * Builds a delete request for the given entity.
     *
     * <p>The full entity is carried in the payload rather than just the id so the
     * consumer can log the full state for audit purposes and so
     * {@link MutableSource#delete(JpaModel)} receives the same shape it would get
     * from a local {@code JpaRepository} lookup.
     *
     * @param entityType the entity class, used only to capture the FQCN
     * @param entity the entity to remove
     * @param gson the producer's Gson instance used to serialize {@code entity}
     * @param sourceId the target {@link MutableSource} id
     * @param <T> the entity type
     * @return a new write request with {@link Operation#DELETE}
     */
    public static <T extends JpaModel> @NotNull WriteRequest delete(
        @NotNull Class<T> entityType,
        @NotNull T entity,
        @NotNull Gson gson,
        @NotNull String sourceId
    ) {
        return new WriteRequest(
            UUID.randomUUID(),
            Instant.now(),
            Operation.DELETE,
            entityType.getName(),
            gson.toJson(entity),
            sourceId
        );
    }

    /**
     * Returns a copy of this request with {@link #requestId} replaced by the
     * given value and every other field preserved byte-identically.
     *
     * <p>Used by retry-escalation paths on the consumer side to rebuild a
     * request while keeping the original producer's request id intact. The
     * static factories {@link #upsert(Class, JpaModel, Gson, String)} and
     * {@link #delete(Class, JpaModel, Gson, String)} mint a fresh random UUID
     * per call, which is correct for new producer puts but wrong for a retry
     * cycle - dead-letter queries and audit trails must correlate end-to-end
     * on the initial producer's request id across every retry attempt.
     *
     * <p>This method replaces an earlier reflection-based rebuilder that the
     * {@code simplified-data WriteBatchScheduler} used to bypass the private
     * constructor before this helper existed.
     *
     * @param requestId the request id to substitute
     * @return a new {@code WriteRequest} with the supplied request id and
     *         every other field copied from this instance
     */
    public @NotNull WriteRequest withRequestId(@NotNull UUID requestId) {
        return new WriteRequest(
            requestId,
            this.timestamp,
            this.operation,
            this.entityClassName,
            this.entityJson,
            this.sourceId
        );
    }

    /**
     * Resolves {@link #entityClassName} to a live {@link JpaModel} subclass via
     * {@link Class#forName(String)}.
     *
     * <p>Callers in the Phase 6 consumer use the returned class to look up a
     * {@link MutableSource} in a per-type registry and to drive
     * {@link #deserializeEntity(Gson, Class)}. A {@link ClassNotFoundException} is
     * wrapped in {@link JpaException} with the offending FQCN in the message, so
     * the consumer can surface it as a single WARN log and skip the request without
     * crashing the drain loop.
     *
     * @return the resolved entity class
     * @throws JpaException if the class cannot be loaded or does not implement
     *                      {@link JpaModel}
     */
    @SuppressWarnings("unchecked")
    public @NotNull Class<? extends JpaModel> resolveEntityType() throws JpaException {
        Class<?> loaded;

        try {
            loaded = Class.forName(this.entityClassName);
        } catch (ClassNotFoundException ex) {
            throw new JpaException(ex, "Cannot resolve WriteRequest entity class '%s'", this.entityClassName);
        }

        if (!JpaModel.class.isAssignableFrom(loaded))
            throw new JpaException("WriteRequest entity class '%s' does not implement JpaModel", this.entityClassName);

        return (Class<? extends JpaModel>) loaded;
    }

    /**
     * Deserializes {@link #entityJson} into a live entity instance using the given
     * {@link Gson}.
     *
     * <p>The {@code targetType} argument is typically the result of
     * {@link #resolveEntityType()} cast to the concrete type the caller knows it is
     * dispatching to. Callers must ensure the target type matches
     * {@link #getEntityClassName()} or the returned object will not round-trip the
     * producer's state.
     *
     * @param gson the consumer's Gson instance
     * @param targetType the concrete entity class to deserialize into
     * @param <T> the entity type
     * @return the rehydrated entity
     * @throws JpaException if Gson cannot parse the stored JSON against the target type
     */
    public <T extends JpaModel> @NotNull T deserializeEntity(@NotNull Gson gson, @NotNull Class<T> targetType) throws JpaException {
        try {
            T result = gson.fromJson(this.entityJson, targetType);

            if (result == null)
                throw new JpaException("Gson deserialized WriteRequest entity to null for type '%s'", targetType.getName());

            return result;
        } catch (JsonSyntaxException ex) {
            throw new JpaException(ex, "Cannot deserialize WriteRequest entity JSON for type '%s'", targetType.getName());
        }
    }

    /**
     * Discriminates between upsert and delete requests.
     *
     * <p>Deliberately minimal - the library does not need to distinguish between
     * insert and update because every {@link MutableSource} treats them identically
     * (see {@link MutableSource#upsert(JpaModel)}). If a future phase adds partial
     * updates or conditional writes, add a new enum constant rather than widening
     * {@code UPSERT}.
     */
    public enum Operation {

        /**
         * Create or replace the entity on the target source. The consumer dispatches
         * to {@link MutableSource#upsert(JpaModel)}.
         */
        UPSERT,

        /**
         * Remove the entity from the target source. The consumer dispatches to
         * {@link MutableSource#delete(JpaModel)}.
         */
        DELETE

    }

}

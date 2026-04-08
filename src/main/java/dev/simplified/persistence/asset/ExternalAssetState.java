package dev.simplified.persistence.asset;

import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Per-source change-detection state for an external asset origin such as
 * {@code skyblock-data}.
 *
 * <p>One row per logical data source. The {@link #sourceId} serves as the natural primary
 * key and is referenced by {@link ExternalAssetEntryState#getSourceId()} (as a loose
 * column reference, not a JPA {@code @ManyToOne} - the poller treats the relationship
 * as flat to avoid eager-loading churn).
 *
 * <p>Phase 4a ships the schema only. Phase 4c populates rows via the scheduled poller
 * pipeline, using {@link #etag} + {@link #commitSha} + {@link #contentSha256} to short-
 * circuit no-op polls and {@link #lastCheckedAt} / {@link #lastSuccessAt} for operator
 * observability.
 *
 * @see ExternalAssetEntryState
 */
@Entity
@Table(name = "external_asset_state")
@Getter
@Setter
@NoArgsConstructor
public class ExternalAssetState implements JpaModel {

    @Id
    @Column(name = "source_id", nullable = false)
    private @NotNull String sourceId = "";

    @Getter(AccessLevel.NONE)
    @Column(name = "etag")
    private @Nullable String etag;

    @Getter(AccessLevel.NONE)
    @Column(name = "commit_sha")
    private @Nullable String commitSha;

    @Getter(AccessLevel.NONE)
    @Column(name = "content_sha256")
    private @Nullable String contentSha256;

    @Column(name = "last_checked_at", nullable = false)
    private @NotNull Instant lastCheckedAt = Instant.EPOCH;

    @Getter(AccessLevel.NONE)
    @Column(name = "last_success_at")
    private @Nullable Instant lastSuccessAt;

    /**
     * Returns the last-seen ETag wrapped in {@link Optional}.
     *
     * @return the ETag, or empty when never observed
     */
    public @NotNull Optional<String> getEtag() {
        return Optional.ofNullable(this.etag);
    }

    /**
     * Returns the last-seen commit sha wrapped in {@link Optional}.
     *
     * @return the commit sha, or empty when never observed
     */
    public @NotNull Optional<String> getCommitSha() {
        return Optional.ofNullable(this.commitSha);
    }

    /**
     * Returns the last-seen content hash wrapped in {@link Optional}.
     *
     * @return the hex SHA-256, or empty when never observed
     */
    public @NotNull Optional<String> getContentSha256() {
        return Optional.ofNullable(this.contentSha256);
    }

    /**
     * Returns the last successful probe timestamp wrapped in {@link Optional}.
     *
     * @return the timestamp, or empty when no probe has succeeded yet
     */
    public @NotNull Optional<Instant> getLastSuccessAt() {
        return Optional.ofNullable(this.lastSuccessAt);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        ExternalAssetState that = (ExternalAssetState) o;

        return Objects.equals(this.sourceId, that.sourceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sourceId);
    }

}

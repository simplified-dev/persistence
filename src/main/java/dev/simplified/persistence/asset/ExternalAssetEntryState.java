package dev.simplified.persistence.asset;

import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Per-entry change-detection state recording the last observed content hash for an
 * individual file inside a {@link ExternalAssetState source}'s manifest.
 *
 * <p>Keyed by the composite {@code (sourceId, entryPath)} via JPA {@link IdClass},
 * matching the locked schema's {@code UNIQUE(source_id, entry_path)} constraint. The
 * {@code sourceId} is a loose reference to {@link ExternalAssetState#getSourceId()}
 * - no JPA {@code @ManyToOne} - because the Phase 4c poller walks entries flat and a
 * strict FK would add eager-loading churn without benefit.
 *
 * <p>Phase 4a ships the schema only. Phase 4c's delta engine populates rows per manifest
 * entry and uses {@link #entrySha256} mismatches to decide which {@code JpaRepository}
 * caches to invalidate.
 *
 * @see ExternalAssetState
 */
@Entity
@Table(name = "external_asset_entry_state")
@IdClass(ExternalAssetEntryState.PK.class)
@Getter
@Setter
@NoArgsConstructor
public class ExternalAssetEntryState implements JpaModel {

    @Id
    @Column(name = "source_id", nullable = false)
    private @NotNull String sourceId = "";

    @Id
    @Column(name = "entry_path", nullable = false)
    private @NotNull String entryPath = "";

    @Column(name = "entry_sha256", nullable = false)
    private @NotNull String entrySha256 = "";

    @Column(name = "last_seen_at", nullable = false)
    private @NotNull Instant lastSeenAt = Instant.EPOCH;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        ExternalAssetEntryState that = (ExternalAssetEntryState) o;

        return Objects.equals(this.sourceId, that.sourceId)
            && Objects.equals(this.entryPath, that.entryPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sourceId, this.entryPath);
    }

    /**
     * Composite primary key for {@link ExternalAssetEntryState}. Required by the JPA
     * {@link IdClass} contract and must be a public, no-arg-constructible, {@link Serializable}
     * class whose field names match the parent entity's {@link Id @Id} fields.
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class PK implements Serializable {

        private @NotNull String sourceId = "";
        private @NotNull String entryPath = "";

    }

}

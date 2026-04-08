package dev.simplified.persistence.asset;

import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link ExternalAssetState} CRUD against an in-memory H2
 * database. The session is built with a dedicated {@link RepositoryFactory} anchored
 * at the {@code asset} package so the new entities register without leaking into
 * the other test suites.
 */
@Tag("slow")
class ExternalAssetStateTest {

    private SessionManager sessionManager;
    private JpaSession session;

    @BeforeEach
    void setup() {
        this.sessionManager = new SessionManager();
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "external_asset_state_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(ExternalAssetState.class)
                    .build()
            )
            .build();
        this.session = this.sessionManager.connect(config);
    }

    @AfterEach
    void teardown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
    }

    @Test
    void persistAndFind() {
        ExternalAssetState state = new ExternalAssetState();
        state.setSourceId("skyblock-data");
        state.setEtag("W/\"abc123\"");
        state.setCommitSha("deadbeef");
        state.setContentSha256("hex...");
        state.setLastCheckedAt(Instant.parse("2026-04-07T00:00:00Z"));
        state.setLastSuccessAt(Instant.parse("2026-04-07T00:00:00Z"));

        this.session.transaction(s -> { s.persist(state); });

        this.session.with(s -> {
            ExternalAssetState loaded = s.find(ExternalAssetState.class, "skyblock-data");
            assertNotNull(loaded);
            assertEquals("skyblock-data", loaded.getSourceId());
            assertEquals("W/\"abc123\"", loaded.getEtag().orElseThrow());
            assertEquals("deadbeef", loaded.getCommitSha().orElseThrow());
            assertEquals("hex...", loaded.getContentSha256().orElseThrow());
            assertEquals(Instant.parse("2026-04-07T00:00:00Z"), loaded.getLastCheckedAt());
            assertEquals(Instant.parse("2026-04-07T00:00:00Z"), loaded.getLastSuccessAt().orElseThrow());
        });
    }

    @Test
    void duplicatePrimaryKeyRejected() {
        ExternalAssetState first = new ExternalAssetState();
        first.setSourceId("dup");
        first.setLastCheckedAt(Instant.EPOCH);

        ExternalAssetState second = new ExternalAssetState();
        second.setSourceId("dup");
        second.setLastCheckedAt(Instant.EPOCH);

        this.session.transaction(s -> { s.persist(first); });

        assertThrows(
            JpaException.class,
            () -> this.session.transaction(s -> { s.persist(second); }),
            "Inserting a second row with the same source_id must violate the primary key constraint"
        );
    }

    @Test
    void nullableFieldsDefaultToEmpty() {
        ExternalAssetState state = new ExternalAssetState();
        state.setSourceId("nullable");
        state.setLastCheckedAt(Instant.EPOCH);

        this.session.transaction(s -> { s.persist(state); });

        this.session.with(s -> {
            ExternalAssetState loaded = s.find(ExternalAssetState.class, "nullable");
            assertTrue(loaded.getEtag().isEmpty());
            assertTrue(loaded.getCommitSha().isEmpty());
            assertTrue(loaded.getContentSha256().isEmpty());
            assertTrue(loaded.getLastSuccessAt().isEmpty());
        });
    }

}

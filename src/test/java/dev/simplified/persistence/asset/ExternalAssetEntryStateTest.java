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

/**
 * Integration test for {@link ExternalAssetEntryState} composite-key CRUD against
 * an in-memory H2 database.
 */
@Tag("slow")
class ExternalAssetEntryStateTest {

    private SessionManager sessionManager;
    private JpaSession session;

    @BeforeEach
    void setup() {
        this.sessionManager = new SessionManager();
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "external_asset_entry_state_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(ExternalAssetEntryState.class)
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
    void persistAndFindByCompositeKey() {
        ExternalAssetEntryState entry = new ExternalAssetEntryState();
        entry.setSourceId("skyblock-data");
        entry.setEntryPath("data/v1/items/items.json");
        entry.setEntrySha256("abc123");
        entry.setLastSeenAt(Instant.parse("2026-04-07T00:00:00Z"));

        this.session.transaction(s -> { s.persist(entry); });

        this.session.with(s -> {
            ExternalAssetEntryState.PK pk = new ExternalAssetEntryState.PK(
                "skyblock-data",
                "data/v1/items/items.json"
            );
            ExternalAssetEntryState loaded = s.find(ExternalAssetEntryState.class, pk);
            assertNotNull(loaded);
            assertEquals("skyblock-data", loaded.getSourceId());
            assertEquals("data/v1/items/items.json", loaded.getEntryPath());
            assertEquals("abc123", loaded.getEntrySha256());
            assertEquals(Instant.parse("2026-04-07T00:00:00Z"), loaded.getLastSeenAt());
        });
    }

    @Test
    void sameSourceDifferentPathCoexist() {
        ExternalAssetEntryState a = new ExternalAssetEntryState();
        a.setSourceId("skyblock-data");
        a.setEntryPath("data/v1/items/items.json");
        a.setEntrySha256("sha-a");
        a.setLastSeenAt(Instant.EPOCH);

        ExternalAssetEntryState b = new ExternalAssetEntryState();
        b.setSourceId("skyblock-data");
        b.setEntryPath("data/v1/mobs/bestiary_families.json");
        b.setEntrySha256("sha-b");
        b.setLastSeenAt(Instant.EPOCH);

        this.session.transaction(s -> {
            s.persist(a);
            s.persist(b);
        });

        this.session.with(s -> {
            assertNotNull(s.find(
                ExternalAssetEntryState.class,
                new ExternalAssetEntryState.PK("skyblock-data", "data/v1/items/items.json")
            ));
            assertNotNull(s.find(
                ExternalAssetEntryState.class,
                new ExternalAssetEntryState.PK("skyblock-data", "data/v1/mobs/bestiary_families.json")
            ));
        });
    }

    @Test
    void duplicateCompositeKeyRejected() {
        ExternalAssetEntryState first = new ExternalAssetEntryState();
        first.setSourceId("dup-source");
        first.setEntryPath("dup-path");
        first.setEntrySha256("sha1");
        first.setLastSeenAt(Instant.EPOCH);

        ExternalAssetEntryState second = new ExternalAssetEntryState();
        second.setSourceId("dup-source");
        second.setEntryPath("dup-path");
        second.setEntrySha256("sha2");
        second.setLastSeenAt(Instant.EPOCH);

        this.session.transaction(s -> { s.persist(first); });

        assertThrows(
            JpaException.class,
            () -> this.session.transaction(s -> { s.persist(second); }),
            "Inserting a second row with the same (source_id, entry_path) must violate the composite primary key"
        );
    }

}

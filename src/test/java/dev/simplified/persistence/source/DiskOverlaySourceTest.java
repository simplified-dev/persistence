package dev.simplified.persistence.source;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.Repository;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.model.TestParentModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DiskOverlaySource}, verifying overlay-wins-when-present
 * behavior, fallback-to-inner when absent, and the explicit NO-CACHING contract
 * (overlay file is re-read on every load).
 */
@Tag("fast")
class DiskOverlaySourceTest {

    private SessionManager sessionManager;
    private JpaSession session;
    private JpaRepository<TestParentModel> repository;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        this.sessionManager = new SessionManager();
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "disk_overlay_source_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(TestParentModel.class)
                    .build()
            )
            .build();
        this.session = this.sessionManager.connect(config);
        Repository<TestParentModel> repo = this.session.getRepository(TestParentModel.class);
        this.repository = (JpaRepository<TestParentModel>) repo;
    }

    @AfterEach
    void teardown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
    }

    @Test
    void load_overlayWinsWhenPresent(@TempDir Path tempDir) throws Exception {
        Path overlay = tempDir.resolve("parents.json");
        Files.writeString(overlay, "[{\"id\":42,\"name\":\"from-overlay\"}]", StandardCharsets.UTF_8);

        AtomicInteger innerCalls = new AtomicInteger();
        Source<TestParentModel> inner = repo -> {
            innerCalls.incrementAndGet();
            return Concurrent.newList();
        };

        DiskOverlaySource<TestParentModel> source = new DiskOverlaySource<>(
            inner, overlay, TestParentModel.class
        );

        ConcurrentList<TestParentModel> result = source.load(this.repository);

        assertEquals(1, result.size());
        assertEquals(42, result.getFirst().getId());
        assertEquals("from-overlay", result.getFirst().getName());
        assertEquals(0, innerCalls.get(), "Inner source should not be consulted when overlay exists");
    }

    @Test
    void load_fallsBackToInnerWhenOverlayAbsent(@TempDir Path tempDir) {
        Path overlay = tempDir.resolve("missing.json");
        // Do not create the file.

        AtomicInteger innerCalls = new AtomicInteger();
        Source<TestParentModel> inner = repo -> {
            innerCalls.incrementAndGet();
            TestParentModel fallback = new TestParentModel();
            fallback.setId(1);
            fallback.setName("from-inner");
            return Concurrent.newList(fallback);
        };

        DiskOverlaySource<TestParentModel> source = new DiskOverlaySource<>(
            inner, overlay, TestParentModel.class
        );

        ConcurrentList<TestParentModel> result = source.load(this.repository);

        assertEquals(1, result.size());
        assertEquals("from-inner", result.getFirst().getName());
        assertEquals(1, innerCalls.get(), "Inner source should be consulted exactly once when overlay is absent");
    }

    @Test
    void load_rereadsOverlayEveryCall(@TempDir Path tempDir) throws Exception {
        Path overlay = tempDir.resolve("parents.json");
        Files.writeString(overlay, "[{\"id\":1,\"name\":\"first\"}]", StandardCharsets.UTF_8);

        Source<TestParentModel> inner = repo -> Concurrent.newList();
        DiskOverlaySource<TestParentModel> source = new DiskOverlaySource<>(
            inner, overlay, TestParentModel.class
        );

        ConcurrentList<TestParentModel> first = source.load(this.repository);
        assertEquals("first", first.getFirst().getName());

        // Mutate the overlay and expect the next load to see the new content.
        Files.writeString(overlay, "[{\"id\":2,\"name\":\"second\"}]", StandardCharsets.UTF_8);

        ConcurrentList<TestParentModel> second = source.load(this.repository);
        assertEquals("second", second.getFirst().getName(),
            "DiskOverlaySource must re-read the overlay on every load (no caching)");
    }

    @Test
    void load_malformedOverlayThrows(@TempDir Path tempDir) throws Exception {
        Path overlay = tempDir.resolve("bad.json");
        Files.writeString(overlay, "this is not json", StandardCharsets.UTF_8);

        Source<TestParentModel> inner = repo -> Concurrent.newList();
        DiskOverlaySource<TestParentModel> source = new DiskOverlaySource<>(
            inner, overlay, TestParentModel.class
        );

        JpaException ex = assertThrows(JpaException.class, () -> source.load(this.repository));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains(TestParentModel.class.getName()));
    }

}

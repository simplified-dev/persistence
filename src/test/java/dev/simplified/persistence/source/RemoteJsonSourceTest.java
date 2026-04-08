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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RemoteJsonSource} that exercise manifest lookup, primary file
 * parsing, {@code _extra} companion merging, and error-path behavior using in-memory
 * {@link IndexProvider} and {@link FileFetcher} stubs.
 */
@Tag("fast")
class RemoteJsonSourceTest {

    private SessionManager sessionManager;
    private JpaSession session;
    private JpaRepository<TestParentModel> repository;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        this.sessionManager = new SessionManager();
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "remote_json_source_test")
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
    void load_parsesPrimaryFile() {
        ManifestIndex manifest = stubManifest(TestParentModel.class, "data/v1/test/parents.json", false);
        IndexProvider indexProvider = () -> manifest;
        FileFetcher fileFetcher = path -> switch (path) {
            case "data/v1/test/parents.json" -> "[{\"id\":1,\"name\":\"parent1\"},{\"id\":2,\"name\":\"parent2\"}]";
            default -> throw new JpaException("unexpected path '%s'", path);
        };

        RemoteJsonSource<TestParentModel> source = new RemoteJsonSource<>(
            "test-source", indexProvider, fileFetcher, TestParentModel.class
        );

        ConcurrentList<TestParentModel> result = source.load(this.repository);

        assertEquals(2, result.size(), "Primary file should parse into two entities");
        assertEquals(1, result.getFirst().getId());
        assertEquals("parent1", result.getFirst().getName());
        assertEquals(2, result.get(1).getId());
        assertEquals("parent2", result.get(1).getName());
    }

    @Test
    void load_mergesExtraCompanion() {
        ManifestIndex manifest = stubManifestWithExtra(
            TestParentModel.class,
            "data/v1/test/parents.json",
            "data/v1/test/parents_extra.json"
        );
        IndexProvider indexProvider = () -> manifest;
        FileFetcher fileFetcher = path -> switch (path) {
            case "data/v1/test/parents.json" -> "[{\"id\":1,\"name\":\"parent1\"}]";
            case "data/v1/test/parents_extra.json" -> "[{\"id\":99,\"name\":\"extra\"}]";
            default -> throw new JpaException("unexpected path '%s'", path);
        };

        RemoteJsonSource<TestParentModel> source = new RemoteJsonSource<>(
            "test-source", indexProvider, fileFetcher, TestParentModel.class
        );

        ConcurrentList<TestParentModel> result = source.load(this.repository);

        assertEquals(2, result.size(), "Extra entries should be appended to primary entries");
        assertEquals(1, result.getFirst().getId());
        assertEquals(99, result.get(1).getId());
    }

    @Test
    void load_missingEntryThrows() {
        ManifestIndex manifest = ManifestIndex.empty();
        IndexProvider indexProvider = () -> manifest;
        FileFetcher fileFetcher = path -> {
            throw new JpaException("should not be called");
        };

        RemoteJsonSource<TestParentModel> source = new RemoteJsonSource<>(
            "test-source", indexProvider, fileFetcher, TestParentModel.class
        );

        JpaException ex = assertThrows(JpaException.class, () -> source.load(this.repository));
        assertNotNull(ex.getMessage());
        assertTrue(ex.getMessage().contains(TestParentModel.class.getName()),
            "Exception should mention the missing model class FQCN");
        assertTrue(ex.getMessage().contains("test-source"),
            "Exception should mention the source id");
    }

    /**
     * Reflectively builds a {@link ManifestIndex} for a single primary-only entry.
     * Reflection is used because {@link ManifestIndex}'s private constructor is
     * package-private-or-narrower by design (Gson deserialization only).
     */
    private static @NotNull ManifestIndex stubManifest(@NotNull Class<?> modelClass, @NotNull String path, boolean hasExtra) {
        return buildManifest(modelClass, path, hasExtra, null);
    }

    private static @NotNull ManifestIndex stubManifestWithExtra(@NotNull Class<?> modelClass, @NotNull String path, @NotNull String extraPath) {
        return buildManifest(modelClass, path, true, extraPath);
    }

    @SuppressWarnings("unchecked")
    private static @NotNull ManifestIndex buildManifest(
        @NotNull Class<?> modelClass,
        @NotNull String path,
        boolean hasExtra,
        @Nullable String extraPath
    ) {
        try {
            Constructor<ManifestIndex.Entry> entryCtor =
                (Constructor<ManifestIndex.Entry>) ManifestIndex.Entry.class.getDeclaredConstructors()[0];
            entryCtor.setAccessible(true);
            ManifestIndex.Entry entry = entryCtor.newInstance(
                path,
                "test",
                "test_table",
                modelClass.getName(),
                "dummy-sha",
                0L,
                hasExtra,
                extraPath,
                hasExtra ? "dummy-extra-sha" : null,
                hasExtra ? 0L : null
            );

            Constructor<ManifestIndex> manifestCtor =
                (Constructor<ManifestIndex>) ManifestIndex.class.getDeclaredConstructors()[0];
            manifestCtor.setAccessible(true);
            return manifestCtor.newInstance(
                1,
                "2026-04-07T00:00:00Z",
                null,
                1,
                Concurrent.newUnmodifiableList(entry)
            );
        } catch (ReflectiveOperationException ex) {
            throw new RuntimeException("Failed to build stub manifest", ex);
        }
    }

}

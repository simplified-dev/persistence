package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.associated.AssociatedRow;
import dev.simplified.persistence.associated.OneToOneOwner;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.source.RelationalSource;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Which relational types a write rebuilds through their single-valued associations: a type pairing
 * with the written one through a one-to-one association, as one associating with it through a
 * many-to-one is.
 */
@Tag("slow")
class JpaSessionAssociationTest {

    private SessionManager sessionManager;
    private RelationalSource database;
    private JpaSession session;

    @BeforeEach
    void connect() {
        ConcurrentList<Class<JpaModel>> mapped = JpaModel.resolveModels(AssociatedRow.class);

        this.sessionManager = new SessionManager();
        this.database = H2MemoryDriver.named("jpa_session_association_test").withModels(mapped).build();
        this.session = this.sessionManager.connect(new JpaConfig(mapped, this.database));

        AssociatedRow row = row(1, "one");
        this.session.write(WriteRequest.upsert(AssociatedRow.class, List.of(row)));

        OneToOneOwner paired = new OneToOneOwner();
        paired.setId(1);
        paired.setRow(row);
        this.session.write(WriteRequest.upsert(OneToOneOwner.class, List.of(paired)));
    }

    @AfterEach
    void shutdown() {
        // The session first, so no write or tick reaches a closed database; then the opener closes
        // what it opened.
        if (this.sessionManager != null)
            this.sessionManager.shutdown();

        if (this.database != null)
            this.database.close();
    }

    @Test
    @DisplayName("a write rebuilds the type pairing with the written one through a one-to-one association")
    void aOneToOneOwnerFollowsTheWrite() {
        assertThat(this.held(OneToOneOwner.class).getRow().getName(), equalTo("one"));

        this.session.write(WriteRequest.upsert(AssociatedRow.class, List.of(row(1, "renamed"))));

        assertThat(this.held(OneToOneOwner.class).getRow().getName(), equalTo("renamed"));
    }

    /**
     * Reads the one row the session holds of a type.
     *
     * @param type the registered type
     * @param <T> the entity type
     * @return the first held row
     */
    private <T extends JpaModel> @NotNull T held(@NotNull Class<T> type) {
        return this.session.getRepository(type).orElseThrow().getRows().getFirst();
    }

    /**
     * Builds an associated row.
     *
     * @param id the row's id
     * @param name the row's name
     * @return the row
     */
    private static @NotNull AssociatedRow row(int id, @NotNull String name) {
        AssociatedRow row = new AssociatedRow();
        row.setId(id);
        row.setName(name);
        return row;
    }

}

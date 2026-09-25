package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.associated.AssociatedRow;
import dev.simplified.persistence.associated.HopOwner;
import dev.simplified.persistence.associated.OneToOneOwner;
import dev.simplified.persistence.associated.UnregisteredHop;
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
import static org.hamcrest.Matchers.is;

/**
 * Which relational types a write rebuilds through their single-valued associations: a type pairing
 * with the written one through a one-to-one association, as one associating with it through a
 * many-to-one is, and a type reaching it only through a type the database maps and the session
 * leaves unregistered.
 */
@Tag("slow")
class JpaSessionAssociationTest {

    private SessionManager sessionManager;
    private RelationalSource database;
    private JpaSession session;

    @BeforeEach
    void connect() {
        ConcurrentList<Class<JpaModel>> mapped = JpaModel.resolveModels(AssociatedRow.class);
        ConcurrentList<Class<JpaModel>> registered = mapped.stream()
            .filter(type -> !UnregisteredHop.class.equals(type))
            .collect(Concurrent.toUnmodifiableList());

        this.sessionManager = new SessionManager();
        this.database = H2MemoryDriver.named("jpa_session_association_test").withModels(mapped).build();
        this.session = this.sessionManager.connect(new JpaConfig(registered, this.database));

        AssociatedRow row = row("one");
        this.session.write(WriteRequest.upsert(AssociatedRow.class, List.of(row)));

        OneToOneOwner paired = new OneToOneOwner();
        paired.setId(1);
        paired.setRow(row);
        this.session.write(WriteRequest.upsert(OneToOneOwner.class, List.of(paired)));

        // The hop is the database's alone, so it is written through the database rather than the
        // session.
        UnregisteredHop hop = new UnregisteredHop();
        hop.setId(1);
        hop.setRow(row);
        this.database.write(WriteRequest.upsert(UnregisteredHop.class, List.of(hop)));

        HopOwner owner = new HopOwner();
        owner.setId(1);
        owner.setHop(hop);
        this.session.write(WriteRequest.upsert(HopOwner.class, List.of(owner)));
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

        this.session.write(WriteRequest.upsert(AssociatedRow.class, List.of(row("renamed"))));

        assertThat(this.held(OneToOneOwner.class).getRow().getName(), equalTo("renamed"));
    }

    @Test
    @DisplayName("a write rebuilds a type reaching the written one only through an unregistered type's eager association")
    void anOwnerBeyondAnUnregisteredHopFollowsTheWrite() {
        assertThat(this.session.getRepository(UnregisteredHop.class).isPresent(), is(false));
        assertThat(this.held(HopOwner.class).getHop().getRow().getName(), equalTo("one"));

        this.session.write(WriteRequest.upsert(AssociatedRow.class, List.of(row("renamed"))));

        assertThat(this.held(HopOwner.class).getHop().getRow().getName(), equalTo("renamed"));
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
     * Builds the associated row every owner here reaches, the one with id {@code 1}.
     *
     * @param name the row's name
     * @return the row
     */
    private static @NotNull AssociatedRow row(@NotNull String name) {
        AssociatedRow row = new AssociatedRow();
        row.setId(1);
        row.setName(name);
        return row;
    }

}

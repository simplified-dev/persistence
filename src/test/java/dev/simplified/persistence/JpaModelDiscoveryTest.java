package dev.simplified.persistence;

import dev.simplified.persistence.unmapped.ContractRow;
import dev.simplified.persistence.unmapped.LayeredRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Model discovery: which types an anchor finds, and the order it answers them in.
 */
class JpaModelDiscoveryTest {

    @Test
    @DisplayName("an anchor finds every model under its package, ordered by name")
    void anchorScansItsPackage() {
        assertThat(JpaModel.resolveModels(ContractRow.class), contains(ContractRow.class, LayeredRow.class));
    }

    @Test
    @DisplayName("discovery is stable, so two scans of one anchor agree")
    void discoveryIsStable() {
        assertThat(
            JpaModel.resolveModels(ContractRow.class),
            equalTo(JpaModel.resolveModels(ContractRow.class))
        );
    }

}

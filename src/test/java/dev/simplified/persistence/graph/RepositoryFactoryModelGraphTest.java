package dev.simplified.persistence.graph;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.RepositoryFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

/**
 * Covers the dependency graph {@link RepositoryFactory#resolveModels(Class)} builds.
 * <p>
 * These two entities live in their own package so no other session anchors on them - the package
 * filter is a prefix match, so a fixture placed beside the shared test models would join every
 * session that scans them.
 */
class RepositoryFactoryModelGraphTest {

    @Test
    @DisplayName("a bidirectional association is not a cycle")
    void resolvesBidirectionalAssociation() {
        // the inverse side used to contribute an edge of its own, which asserted the opposite
        // ordering to the owning side and left the sort with a contradiction to reject
        ConcurrentList<Class<JpaModel>> models = RepositoryFactory.resolveModels(GraphOwnerModel.class);

        assertThat(models, hasSize(2));
        // the dependent carries the foreign key, so the entity it points at is registered first
        assertThat(models, contains(GraphOwnerModel.class, GraphDependentModel.class));
        assertThat(models.indexOf(GraphOwnerModel.class), is(lessThan(models.indexOf(GraphDependentModel.class))));
    }

}

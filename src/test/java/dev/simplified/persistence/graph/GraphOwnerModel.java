package dev.simplified.persistence.graph;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The referenced half of a bidirectional association, carrying only the inverse side.
 *
 * @see GraphDependentModel
 */
@Entity
@Table(name = "graph_owner")
@Getter
@Setter
public class GraphOwnerModel implements JpaModel {

    @Id
    @Column(name = "id")
    private int id;

    @OneToMany(mappedBy = "owner")
    private ConcurrentList<GraphDependentModel> dependents = Concurrent.newList();

}

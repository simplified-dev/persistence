package dev.simplified.persistence.graph;

import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * The referencing half of a bidirectional association, carrying the owning side and the foreign key.
 *
 * @see GraphOwnerModel
 */
@Entity
@Table(name = "graph_dependent")
@Getter
@Setter
public class GraphDependentModel implements JpaModel {

    @Id
    @Column(name = "id")
    private int id;

    @ManyToOne
    @JoinColumn(name = "owner_id")
    private GraphOwnerModel owner;

}

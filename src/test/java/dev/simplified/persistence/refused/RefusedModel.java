package dev.simplified.persistence.refused;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.unmapped.ContractRow;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * An entity whose association targets a class that is not an entity, which Hibernate refuses when
 * it builds the metadata - so opening a database that maps it fails part way.
 */
@Entity
@Getter
@Setter
@Table(name = "refused")
public class RefusedModel implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    @Column(name = "id")
    private int id;

    /**
     * An association to a type no database maps.
     */
    @ManyToOne
    @JoinColumn(name = "other_id")
    private ContractRow other;

}

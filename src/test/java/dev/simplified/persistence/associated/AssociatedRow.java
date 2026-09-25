package dev.simplified.persistence.associated;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

/**
 * A row other rows associate with, and the one a write renames.
 */
@Entity
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = "associated_row")
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class AssociatedRow implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    @Column(name = "id")
    private int id;

    /**
     * The row's name.
     */
    @Column(name = "name")
    private String name;

}

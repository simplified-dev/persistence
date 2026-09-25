package dev.simplified.persistence.associated;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

/**
 * A row paired with one {@link AssociatedRow} through an eager one-to-one association.
 */
@Entity
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = "one_to_one_owner")
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class OneToOneOwner implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    @Column(name = "id")
    private int id;

    /**
     * The row this one is paired with, loaded with it.
     */
    @OneToOne
    @JoinColumn(name = "row_id")
    private AssociatedRow row;

}

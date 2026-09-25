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
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

/**
 * A row reaching an {@link AssociatedRow} only through the {@link UnregisteredHop} it associates
 * with.
 */
@Entity
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = "hop_owner")
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class HopOwner implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    @Column(name = "id")
    private int id;

    /**
     * The hop this row belongs to, loaded with it together with the row the hop belongs to.
     */
    @ManyToOne
    @JoinColumn(name = "hop_id")
    private UnregisteredHop hop;

}

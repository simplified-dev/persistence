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
 * A row the database maps and a session leaves unregistered, associating eagerly with an
 * {@link AssociatedRow}.
 */
@Entity
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = "unregistered_hop")
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class UnregisteredHop implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    @Column(name = "id")
    private int id;

    /**
     * The row this one belongs to, loaded with it.
     */
    @ManyToOne
    @JoinColumn(name = "row_id")
    private AssociatedRow row;

}

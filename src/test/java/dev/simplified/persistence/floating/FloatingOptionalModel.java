package dev.simplified.persistence.floating;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.Optional;

/**
 * An entity holding the two optional inner types whose native SQL type carries no length, kept in
 * a package of its own so the session that mounts it is the only one that has to survive it.
 */
@Entity
@Table(name = "floating_optional")
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class FloatingOptionalModel implements JpaModel {

    @Id
    @Column(name = "id")
    private int id;

    @Column(name = "opt_double")
    private Optional<Double> optDouble = Optional.empty();

    @Column(name = "opt_float")
    private Optional<Float> optFloat = Optional.empty();

}

package dev.simplified.persistence.model;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;

/**
 * A row for exercising the contracts, carrying no mapping.
 *
 * <p>Nothing in this module reaches an ORM, so the fixture it tests against declares none either -
 * a mapped fixture here would put the annotations back on the compile classpath the split exists to
 * clear.
 */
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class ContractRow implements JpaModel {

    /**
     * The row's identifier.
     */
    private int id;

    /**
     * The row's name.
     */
    private String name;

}

package dev.simplified.persistence.unmapped;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;

/**
 * A row a factory holds without a database behind it, carrying no mapping.
 *
 * <p>A repository is registered against a type, not against a table, so the fixture that exercises
 * registration declares no mapping either. It sits apart from the mapped fixtures because a scan
 * anchored on those would otherwise hand an unmapped class to Hibernate.
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

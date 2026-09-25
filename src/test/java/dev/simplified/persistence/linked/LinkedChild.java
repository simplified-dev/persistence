package dev.simplified.persistence.linked;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row linking to a {@link LinkedParent} through the id it carries.
 */
@Getter
@Setter
public class LinkedChild implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the parent this row links to.
     */
    private @NotNull String parentId = "";

    /**
     * The parent {@link #parentId} names, resolved when the generation is built.
     */
    @Linked("parentId")
    private transient LinkedParent parent;

}

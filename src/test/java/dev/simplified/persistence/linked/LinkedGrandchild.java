package dev.simplified.persistence.linked;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row linking to a {@link LinkedChild}, and so reaching a {@link LinkedParent} through it.
 */
@Getter
@Setter
public class LinkedGrandchild implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the child this row links to.
     */
    private @NotNull String childId = "";

    /**
     * The child {@link #childId} names, resolved when the generation is built.
     */
    @Linked("childId")
    private transient LinkedChild child;

}

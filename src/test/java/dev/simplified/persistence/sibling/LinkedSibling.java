package dev.simplified.persistence.sibling;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row linking to another row of its own type, kept in a package of its own so no other session's
 * scan registers it.
 */
@Getter
@Setter
public class LinkedSibling implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the sibling this row links to.
     */
    private @NotNull String siblingId = "";

    /**
     * The sibling {@link #siblingId} names, resolved when the generation is built.
     */
    @Linked("siblingId")
    private transient LinkedSibling sibling;

}

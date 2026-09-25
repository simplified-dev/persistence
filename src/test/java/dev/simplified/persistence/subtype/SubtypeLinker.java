package dev.simplified.persistence.subtype;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row linking through the id it carries to a {@link SubtypeBase}, which resolves to the registered
 * {@link SubtypeRow}.
 */
@Getter
@Setter
public class SubtypeLinker implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the row this one links to.
     */
    private @NotNull String rowId = "";

    /**
     * The row {@link #rowId} names, declared as the supertype and resolved when the generation is
     * built.
     */
    @Linked("rowId")
    private transient SubtypeBase row;

}

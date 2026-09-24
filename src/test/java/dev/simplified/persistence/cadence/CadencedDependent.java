package dev.simplified.persistence.cadence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row declaring no cadence of its own, linking to a {@link CadencedRow} through the id it carries.
 */
@Getter
@Setter
public class CadencedDependent implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the cadenced row this row links to.
     */
    private @NotNull String rowId = "";

    /**
     * The row {@link #rowId} names, resolved when the generation is built.
     */
    @Linked("rowId")
    private transient CadencedRow row;

}

package dev.simplified.persistence.cycle;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * The other side of a pair of models that link to each other.
 */
@Getter
@Setter
public class CycleB implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the {@link CycleA} this row links to.
     */
    private @NotNull String partnerId = "";

    /**
     * The row {@link #partnerId} names, resolved when the generation is built.
     */
    @Linked("partnerId")
    private transient CycleA partner;

}

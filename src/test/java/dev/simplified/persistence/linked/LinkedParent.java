package dev.simplified.persistence.linked;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row other rows link to, read from a source with no database behind it.
 */
@Getter
@Setter
public class LinkedParent implements JpaModel {

    /**
     * The row's identifier, which a {@link LinkedChild} names to link here.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The row's name, so a rebuilt parent is visible as a changed value.
     */
    private @NotNull String name = "";

}

package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A row holding an element collection, which a session refuses to register.
 */
@Getter
public class ElementOwner implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The tags this row carries, held in a table of their own.
     */
    @ElementCollection
    private @NotNull List<String> tags = List.of();

}

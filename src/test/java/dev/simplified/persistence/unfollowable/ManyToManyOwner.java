package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A row holding a many-to-many association, which a session refuses to register.
 */
@Getter
public class ManyToManyOwner implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The parents this row shares with other rows, loaded with it rather than through a link.
     */
    @ManyToMany
    private @NotNull List<LinkedParent> parents = List.of();

}

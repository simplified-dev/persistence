package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * A row holding a collection-valued association, which a session refuses to register.
 */
@Getter
public class CollectionOwner implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The parents this row owns, loaded with it rather than through a link.
     */
    @OneToMany
    private @NotNull List<LinkedParent> parents = List.of();

}

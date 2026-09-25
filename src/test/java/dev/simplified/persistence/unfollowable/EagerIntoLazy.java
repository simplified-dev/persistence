package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import org.jetbrains.annotations.NotNull;

/**
 * A row whose eager association reaches a {@link LazyChild}, which carries a lazy association of its
 * own, so a session refuses to register it.
 */
@Getter
public class EagerIntoLazy implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The child this row belongs to, loaded with it.
     */
    @ManyToOne
    private LazyChild child;

}

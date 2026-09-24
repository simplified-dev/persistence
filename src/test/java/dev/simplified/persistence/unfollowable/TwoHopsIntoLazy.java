package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;

/**
 * A row whose eager association reaches an {@link EagerIntoLazy}, and through it a lazy association
 * two hops away, so a session refuses to register it.
 */
@Getter
public class TwoHopsIntoLazy implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The row this one is paired with, loaded with it.
     */
    @OneToOne
    private EagerIntoLazy hop;

}

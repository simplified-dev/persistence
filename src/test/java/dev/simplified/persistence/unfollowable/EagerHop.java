package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;

/**
 * A row reached through an eager association and left unregistered, whose own associations are
 * eager - one into a {@link LinkedParent}, one back into its own type.
 */
@Getter
public class EagerHop implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The parent this row belongs to, loaded with it.
     */
    @ManyToOne
    private LinkedParent parent;

    /**
     * The hop after this one, loaded with it.
     */
    @OneToOne
    private EagerHop next;

}

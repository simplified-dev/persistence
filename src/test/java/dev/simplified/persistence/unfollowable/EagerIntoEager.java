package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import org.jetbrains.annotations.NotNull;

/**
 * A row whose eager association reaches an {@link EagerHop}, every association of which is eager as
 * well, so a session registers it.
 */
@Getter
public class EagerIntoEager implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The hop this row belongs to, loaded with it.
     */
    @ManyToOne
    private EagerHop hop;

}

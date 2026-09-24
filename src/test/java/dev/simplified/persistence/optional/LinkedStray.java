package dev.simplified.persistence.optional;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

/**
 * A row whose link to a {@link LinkedParent} may name nothing, kept out of the linked package so a
 * session over that package never registers it.
 */
@Getter
@Setter
public class LinkedStray implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The id of the parent this row links to, empty when it names none.
     */
    private @NotNull Optional<String> parentId = Optional.empty();

    /**
     * The parent {@link #parentId} names, empty when it names none or no parent carries it.
     */
    @Linked("parentId")
    private transient @NotNull Optional<LinkedParent> parent = Optional.empty();

}

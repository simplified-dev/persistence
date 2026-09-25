package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.Linked;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row whose linked list names its element type through a wildcard, which a session refuses to
 * register.
 */
@Getter
public class WildcardLinked implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The ids of the parents this row links to.
     */
    private @NotNull ConcurrentList<String> ids = Concurrent.newList();

    /**
     * The parents the {@link #ids} name, declared through a wildcard.
     */
    @Linked("ids")
    private transient @NotNull ConcurrentList<? extends LinkedParent> parents = Concurrent.newList();

}

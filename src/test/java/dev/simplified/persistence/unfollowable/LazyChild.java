package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import org.jetbrains.annotations.NotNull;

/**
 * A row whose single-valued association is fetched lazily, which a session refuses to register.
 */
@Getter
public class LazyChild implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The parent this row belongs to, loaded only when first read.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    private LinkedParent parent;

}

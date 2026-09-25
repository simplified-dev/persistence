package dev.simplified.persistence.unfollowable;

import dev.simplified.annotations.Getter;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.linked.LinkedParent;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;

/**
 * A row whose one-to-one association is fetched lazily, which a session refuses to register.
 */
@Getter
public class LazyOneToOne implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The parent this row is paired with, loaded only when first read.
     */
    @OneToOne(fetch = FetchType.LAZY)
    private LinkedParent parent;

}

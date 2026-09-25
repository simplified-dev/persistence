package dev.simplified.persistence.subtype;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

/**
 * A row type a link can name, which a session registers only through its subtype
 * {@link SubtypeRow}.
 */
@Getter
@Setter
public abstract class SubtypeBase implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The row's name.
     */
    private @NotNull String name = "";

}

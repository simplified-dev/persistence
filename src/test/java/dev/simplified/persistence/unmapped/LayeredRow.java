package dev.simplified.persistence.unmapped;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.jetbrains.annotations.NotNull;

/**
 * A row published as a document made of layers.
 *
 * <p>It declares a table and an id and nothing else, because those two are the whole of what a
 * document source reads off a model: the name it is published under, and the key its layers merge on.
 */
@Getter
@Setter
@Table(name = "layered")
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class LayeredRow implements JpaModel {

    /**
     * The row's identifier, and the key a later layer overrides on.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The row's name, so an override is visible as a changed value rather than only a changed count.
     */
    private @NotNull String name = "";

}

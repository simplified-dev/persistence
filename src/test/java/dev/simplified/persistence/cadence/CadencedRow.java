package dev.simplified.persistence.cadence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A row rebuilt in the background every 50 milliseconds, read from a source with no database behind
 * it.
 */
@Getter
@Setter
@Hydration(every = 50, unit = TimeUnit.MILLISECONDS)
public class CadencedRow implements JpaModel {

    /**
     * The row's identifier, which a {@link CadencedDependent} names to link here.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The row's name, so a change at the origin is visible as a changed value.
     */
    private @NotNull String name = "";

}

package dev.simplified.persistence.cadence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A row rebuilt in the background every 230 milliseconds beside a {@link TickRow}, so its cadence
 * falls between one tick and the next: a tick finds it not yet due, and the one after picks it up.
 */
@Getter
@Setter
@Hydration(every = 230, unit = TimeUnit.MILLISECONDS)
public class OffTickRow implements JpaModel {

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

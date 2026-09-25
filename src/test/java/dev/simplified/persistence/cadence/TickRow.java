package dev.simplified.persistence.cadence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A row rebuilt in the background every 200 milliseconds, which sets its session's tick, and whose
 * read takes 100 milliseconds, so every tick runs that long.
 */
@Getter
@Setter
@Hydration(every = 200, unit = TimeUnit.MILLISECONDS)
public class TickRow implements JpaModel {

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

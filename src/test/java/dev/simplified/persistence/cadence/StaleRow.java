package dev.simplified.persistence.cadence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A row rebuilt in the background every hour, whose generation is stale 50 milliseconds after it is
 * published - long before any tick comes due.
 */
@Getter
@Setter
@Hydration(every = 3_600_000, stale = 50, unit = TimeUnit.MILLISECONDS)
public class StaleRow implements JpaModel {

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

package dev.simplified.persistence.cadence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Id;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;

/**
 * A row checked in the background every 50 milliseconds, whose generation reports stale only after
 * 400 milliseconds unchecked, so a check that a pause of the test JVM delays does not read as a
 * cadence that stopped.
 */
@Getter
@Setter
@Hydration(every = 50, stale = 400, unit = TimeUnit.MILLISECONDS)
public class CheckedRow implements JpaModel {

    /**
     * The row's identifier, which a {@link CheckedDependent} names to link here.
     */
    @Id
    private @NotNull String id = "";

    /**
     * The row's name.
     */
    private @NotNull String name = "";

}

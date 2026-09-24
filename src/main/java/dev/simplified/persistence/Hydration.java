package dev.simplified.persistence;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Declares when a type's rows are rebuilt in the background.
 *
 * <p>Absence means the type has no cadence of its own. It is read when its session connects, and
 * rebuilt when it or a type it links into is written through the session, or when a type it links
 * into comes due on its own cadence. A change made at the origin by anything else reaches it at the
 * next of those rebuilds, or at the next connect. A type asks for a cadence when it has a reason to,
 * rather than inheriting one it never chose.
 *
 * <pre>{@code
 * @Hydration(every = 6, unit = TimeUnit.HOURS)
 * public class Item implements JpaModel { }
 * }</pre>
 *
 * <p>Only the {@link JpaSession} registering the type acts on this. It ticks at the shortest cadence
 * its types declare, and each tick rebuilds every type whose generation has stood for its
 * {@link #every()}, together with every type linking into it, so a change at the origin reaches a
 * cadenced type with no write. A generation the last rebuild published reports
 * {@link HydrationState#STALE} once it stands past its {@link #stale()} threshold, until a rebuild
 * publishes the next, so a cadence that has stopped shows on read. Nothing downstream can force a
 * rebuild, so a consumer that wants fresher rows declares a shorter cadence rather than reaching for a
 * refresh method.
 *
 * @see HydrationState
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Hydration {

    /**
     * How long to wait between rebuilds, or {@code 0} for no background cadence.
     */
    long every() default 0;

    /**
     * The time unit {@link #every()} and {@link #stale()} are counted in.
     */
    @NotNull TimeUnit unit() default TimeUnit.MINUTES;

    /**
     * How long a generation may stand before it reports {@link HydrationState#STALE}, or {@code 0} to
     * take {@link #every()} plus twice the interval its session ticks at, which a generation outlives
     * only when the rebuilds fall a tick behind or the cadence stops.
     */
    long stale() default 0;

}

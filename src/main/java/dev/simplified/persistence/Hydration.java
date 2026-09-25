package dev.simplified.persistence;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Declares how often a type's rows are checked against their origin in the background.
 *
 * <p>Absence means the type has no cadence of its own. It is read when its session connects, and
 * rebuilt when it or a type it links into is written through the session, or when a type it links
 * into comes due on its own cadence and has moved. A change made at the origin by anything else
 * reaches it at the next of those rebuilds, or at the next connect. A type asks for a cadence when it
 * has a reason to - an origin that other hands change, such as a published corpus - rather than
 * inheriting one it never chose.
 *
 * <pre>{@code
 * @Hydration(every = 6, unit = TimeUnit.HOURS)
 * public class Item implements JpaModel { }
 * }</pre>
 *
 * <p>Only the {@link JpaSession} registering the type acts on this. It ticks at the shortest cadence
 * its types declare, and each tick takes every type that has gone unchecked for its {@link #every()}
 * and asks the source for its fingerprint, which moves whenever the rows the origin holds for the
 * type do. A due type whose fingerprint has not moved since its generation was read is not read
 * again: the check alone restarts its cadence, and {@link Repository#getHydratedAt()} keeps
 * answering when that generation was published. A due type whose fingerprint moved, that the source
 * cannot fingerprint, or whose last rebuild failed is rebuilt together with every type linking into
 * it. So a change at the origin reaches a cadenced type with no write, a tick against an origin that
 * has not moved reads nothing, and a source that fingerprints nothing rebuilds every due type.
 *
 * <p>A generation reports {@link HydrationState#STALE} once it has gone unchecked past its
 * {@link #stale()} threshold, until a tick confirms it or a rebuild publishes the next, so a cadence
 * that has stopped shows on read while one that keeps finding its origin unmoved does not. Nothing
 * downstream can force a rebuild, so a consumer that wants fresher rows declares a shorter cadence
 * rather than reaching for a refresh method.
 *
 * @see HydrationState
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Hydration {

    /**
     * How long to wait between checks, or {@code 0} for no background cadence.
     */
    long every() default 0;

    /**
     * The time unit {@link #every()} and {@link #stale()} are counted in.
     */
    @NotNull TimeUnit unit() default TimeUnit.MINUTES;

    /**
     * How long a generation may go unchecked before it reports {@link HydrationState#STALE}, or
     * {@code 0} to take {@link #every()} plus twice the interval its session ticks at, which a
     * generation outlives only when the ticks fall behind or the cadence stops.
     */
    long stale() default 0;

}

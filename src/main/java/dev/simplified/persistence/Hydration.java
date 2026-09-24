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
 * <p>Absence means hydrate once and never again, which is the right answer for a corpus that changes
 * on someone else's schedule and is told about it. A type asks for a cadence when it has a reason to,
 * rather than inheriting one it never chose.
 *
 * <pre>{@code
 * @Hydration(every = 6, unit = TimeUnit.HOURS)
 * public class Item implements JpaModel { }
 * }</pre>
 *
 * <p>Only the {@link JpaSession} registering the type acts on this. Nothing downstream can force a
 * rebuild, so a consumer that wants fresher rows declares a shorter cadence rather than reaching for a
 * refresh method.
 *
 * @see HydrationState
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Hydration {

    /**
     * How long to wait between rebuilds, or {@code 0} to hydrate once and never again.
     */
    long every() default 0;

    /**
     * The time unit {@link #every()} and {@link #stale()} are counted in.
     */
    @NotNull TimeUnit unit() default TimeUnit.MINUTES;

    /**
     * How long a generation may stand before it reports {@link HydrationState#STALE}, or {@code 0} to
     * take twice {@link #every()}.
     */
    long stale() default 0;

    /**
     * Whether a session waits for this type's first generation before it hands back repositories.
     *
     * <p>Set {@code false} for a type whose first hydration is slow and whose readers can afford to
     * block on first access instead. It says nothing about what a reader does - that is fixed by
     * {@link HydrationState}.
     */
    boolean blocking() default true;

}

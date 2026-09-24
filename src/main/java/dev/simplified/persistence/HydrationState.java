package dev.simplified.persistence;

/**
 * The point a {@link Repository}'s generation has reached in its hydration lifecycle.
 *
 * <p>A session is handed back only once every type it registers holds a generation, and a rebuild
 * that fails after that leaves the type {@link #DEGRADED}, so a reader meets {@link #CURRENT},
 * {@link #REFRESHING} and {@link #DEGRADED}, and {@link #STALE} for a type declaring
 * {@link Hydration}. {@link #UNHYDRATED}, {@link #HYDRATING} and {@link #FAILED} are the
 * states of a first hydration, which a caller never sees. Scope is per type, so one failing type is
 * visible without hiding which one it is.
 *
 * <p>The distinctions this carries that a boolean cannot. {@link #REFRESHING} is a generation being
 * rebuilt while the existing one still answers, which is the ordinary state of every rebuild and never
 * blocks a reader. {@link #DEGRADED} against {@link #FAILED} is the same failure with and without
 * something to serve, and collapsing the two is how a failing origin becomes indistinguishable from an
 * empty corpus. {@link #STALE} against {@link #DEGRADED} separates a stalled cadence from a failing
 * origin - both serve old rows, and the fix differs.
 */
public enum HydrationState {

    /**
     * Registered, holding no generation, with no attempt started.
     */
    UNHYDRATED,

    /**
     * A first hydration is in flight and there is nothing to serve.
     */
    HYDRATING,

    /**
     * A generation exists, the last rebuild published it, and it has not stood past its stale
     * threshold.
     */
    CURRENT,

    /**
     * A generation exists and a rebuild is in flight.
     */
    REFRESHING,

    /**
     * A generation exists, the last rebuild published it, and the generation has stood past its stale
     * threshold.
     */
    STALE,

    /**
     * The last attempt failed and a prior generation is still served.
     */
    DEGRADED,

    /**
     * The last attempt failed and there is no generation to serve.
     */
    FAILED

}

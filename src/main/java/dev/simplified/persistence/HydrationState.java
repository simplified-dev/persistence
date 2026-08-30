package dev.simplified.persistence;

/**
 * The point a {@link Repository}'s generation has reached in its hydration lifecycle.
 *
 * <p>A reader blocks on {@link #UNHYDRATED} and {@link #HYDRATING}, throws on {@link #FAILED}, and is
 * answered immediately on everything else. Scope is per type, and a session-level view is the worst of
 * its repositories, so one failing type is visible without hiding which one it is.
 *
 * <p>The distinctions this carries that a boolean cannot. {@link #REFRESHING} is a generation being
 * rebuilt while the existing one still answers, which is the ordinary state under a wired cadence and
 * must never block. {@link #DEGRADED} against {@link #FAILED} is the same failure with and without
 * something to serve, and collapsing the two is how a failing origin becomes indistinguishable from an
 * empty corpus. {@link #STALE} against {@link #DEGRADED} separates a stalled scheduler from a failing
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
     * A generation exists and is within its freshness window.
     */
    CURRENT,

    /**
     * A generation exists and a rebuild is in flight.
     */
    REFRESHING,

    /**
     * A generation exists, is past its freshness window, and no rebuild is running.
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

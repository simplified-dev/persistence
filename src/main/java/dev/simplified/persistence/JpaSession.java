package dev.simplified.persistence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Log;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.reflection.Reflection;
import dev.simplified.scheduler.Scheduler;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A JPA session holding one {@link JpaRepository} per registered type, every one of them read from the
 * single {@link Source} its {@link JpaConfig} names.
 *
 * <p>The session is the hydrator: it is the only thing that reads a type's rows, links them and
 * publishes them. Each rebuild runs three passes over the types it covers - read every one, link every
 * one, then publish every one - so a link always reaches rows that have been read, and no reader sees
 * a row before its links resolve. Rebuilds run one at a time.
 *
 * <p>A rebuild covers the types asked for and every registered type that links into one of them,
 * directly or through another, whether by {@link Linked} or by a single-valued JPA association. Once
 * the rebuild completes, a {@link Linked} field holds the instance its target's repository holds, and
 * an association holds a copy read with its owner that carries the target's current row. Publication
 * is per type, so a reader between two types' publication sees one new generation and one old.
 *
 * <p>The session records the fingerprint its source answered for each type before the read that
 * produced the held generation. A {@link Hydration} tick asks again and reads only the due types
 * whose fingerprint moved, so a tick against an origin that has not moved reads nothing. A source
 * that cannot fingerprint answers nothing, and every due type is read.
 *
 * <p>The session never asks what kind of source it holds, so nothing here reaches Hibernate. A
 * database is opened by the caller, who keeps it for Hibernate access. Closing it is optional - the
 * JVM closes one still open at exit - and a caller closing it earlier shuts this session down first,
 * because a session reading a closed database fails its next write, rebuild or tick.
 *
 * <p>Typical lifecycle managed by {@link SessionManager}:</p>
 * <ol>
 *     <li><b>Construction</b> - no I/O</li>
 *     <li>{@link #cacheRepositories()} - creates a {@link JpaRepository} per registered type, asks the
 *         source for their fingerprints, hydrates every one of them, and builds the scheduler that
 *         ticks when some type declares a {@link Hydration} cadence</li>
 *     <li>{@link #shutdown()} - clears repositories and shuts down the scheduler, if one was built</li>
 * </ol>
 *
 * <p>Provides repository lookup via {@link #getRepository(Class)} and writes via
 * {@link #write(WriteRequest)}.</p>
 *
 * @see JpaConfig
 * @see JpaRepository
 * @see SessionManager
 */
@Log
public final class JpaSession {

    /**
     * Repositories keyed by their registered entity class.
     */
    private final @NotNull ConcurrentMap<Class<JpaModel>, JpaRepository<JpaModel>> repositories = Concurrent.newMap();

    /**
     * The registered models and the one source they are read from.
     */
    private final @NotNull JpaConfig config;

    /**
     * Every registered type linking into each registered type, directly or through another.
     */
    private final @NotNull ConcurrentMap<Class<JpaModel>, ConcurrentSet<Class<JpaModel>>> dependents;

    /**
     * The fingerprint the source answered for each type before the read that produced its held
     * generation, absent where the source answered none or a write has rebuilt the type since.
     */
    private final @NotNull ConcurrentMap<Class<JpaModel>, String> fingerprints = Concurrent.newMap();

    /**
     * The scheduler driving {@link Hydration} cadences, built only when some registered type declares
     * one.
     */
    private @Nullable Scheduler scheduler;

    /**
     * {@code true} while this session has not been shut down.
     */
    @Getter private volatile boolean active = true;

    /**
     * Constructs a session over the given configuration, performing no I/O.
     *
     * @param config the registered models and the source they are read from
     */
    JpaSession(@NotNull JpaConfig config) {
        this.config = config;
        this.dependents = dependentsOf(config.models());
    }

    /**
     * Creates a {@link JpaRepository} for each registered model and performs the initial data load.
     *
     * <p>The source is asked for every type's fingerprint before the read, and the answer is recorded
     * once the read succeeds. Asked after it, the answer could name a change that landed during the
     * read and that the read never saw, and the first tick would take the stale rows as current.
     *
     * <p>Once every type holds a generation, a session with a type declaring a {@link Hydration}
     * cadence builds its scheduler and ticks at the shortest cadence declared. Called once, by
     * {@link SessionManager#connect(JpaConfig)}.
     *
     * @throws JpaException if the source cannot be asked, or any registered type fails to hydrate
     */
    void cacheRepositories() {
        // A type asks for a cadence through @Hydration. The tick runs at the shortest declared
        // interval and checks only what has come due, rebuilding what moved with every type
        // linking into it, and each repository measures its default stale threshold in ticks.
        long tickMs = this.config.models()
            .stream()
            .mapToLong(model -> JpaRepository.intervalOf(model).toMillis())
            .filter(ms -> ms > 0)
            .min()
            .orElse(0);

        for (Class<JpaModel> model : this.config.models())
            this.repositories.put(model, new JpaRepository<>(model, Duration.ofMillis(tickMs)));

        ConcurrentMap<Class<? extends JpaModel>, String> asked = this.config.source().fingerprints(this.config.models());
        this.hydrate(this.config.models());
        this.record(asked, this.config.models());

        if (tickMs > 0) {
            this.scheduler = new Scheduler();
            this.scheduler.scheduleAsync(this::hydrateDue, tickMs, tickMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Rebuilds the given types and every registered type linking into them, in three passes: read
     * every one, link every one, publish every one.
     *
     * <p>A failing type aborts the rebuild before anything is published, and every type it covered
     * records the failure. Publishing the rest would hand out rows whose links point into a type that
     * never read, which is a wrong answer rather than a missing one.
     *
     * <p>Rebuilds run one at a time, so a write and a due tick never interleave their passes, and a
     * shutdown waits for the rebuild in flight. A session that has been shut down rebuilds nothing.
     *
     * @param asked the registered types the connect, a write or a tick asks to rebuild
     * @throws JpaException if any covered type fails to read or link
     */
    private synchronized void hydrate(@NotNull ConcurrentList<Class<JpaModel>> asked) {
        if (!this.active)
            return;

        ConcurrentList<Class<JpaModel>> types = this.covering(asked);
        ConcurrentMap<Class<JpaModel>, ConcurrentList<JpaModel>> pass = Concurrent.newMap();
        ConcurrentMap<Class<? extends JpaModel>, ConcurrentMap<String, ? extends JpaModel>> keyed = Concurrent.newMap();

        try {
            for (Class<JpaModel> type : types)
                pass.put(type, this.repositories.get(type).hydrate(this.config.source()));

            for (Class<JpaModel> type : types)
                this.repositories.get(type).link(pass.get(type), target -> keyed.computeIfAbsent(target, key -> this.lookupFor(key, pass)));
        } catch (RuntimeException | Error exception) {
            types.forEach(type -> this.repositories.get(type).fail());
            throw exception;
        }

        for (Class<JpaModel> type : types)
            this.repositories.get(type).hold(pass.get(type));
    }

    /**
     * Lists the types a rebuild of the given ones covers: each of them and every registered type
     * linking into one of them.
     *
     * @param asked the registered types asked to rebuild
     * @return the covered types, in registration order
     */
    private @NotNull ConcurrentList<Class<JpaModel>> covering(@NotNull ConcurrentList<Class<JpaModel>> asked) {
        ConcurrentSet<Class<JpaModel>> covered = Concurrent.newSet();

        asked.forEach(type -> {
            covered.add(type);
            covered.addAll(this.dependents.getOrDefault(type, Concurrent.newSet()));
        });

        return this.config.models()
            .stream()
            .filter(covered::contains)
            .collect(Concurrent.toList());
    }

    /**
     * Keys a link target's rows, taking them from the current pass when the target is being rebuilt
     * and from its published generation otherwise.
     *
     * @param target the type a link resolves to
     * @param pass the rows read by the current rebuild, keyed by type
     * @return the target's rows keyed by their stringified id
     * @throws JpaException if no registered type answers for the target
     */
    private @NotNull ConcurrentMap<String, JpaModel> lookupFor(
        @NotNull Class<? extends JpaModel> target,
        @NotNull ConcurrentMap<Class<JpaModel>, ConcurrentList<JpaModel>> pass
    ) {
        Class<JpaModel> type = registered(this.config.models(), target).orElseThrow(
            () -> new JpaException("A link reaches '%s', which this session does not register", target.getName())
        );

        ConcurrentList<JpaModel> rows = pass.containsKey(type) ? pass.get(type) : this.repositories.get(type).getRows();
        return JpaModel.keyed(type, rows);
    }

    /**
     * Checks every type whose {@link Hydration} cadence has come due against its source, and
     * rebuilds the ones that moved together with every type linking into them.
     *
     * <p>The source is asked once for the fingerprints of the due types and every type linking into
     * them. A due type whose fingerprint equals the one its held generation was read under, and which
     * reports {@link HydrationState#CURRENT} or {@link HydrationState#STALE}, is not read: it is
     * confirmed, which restarts its cadence and its stale threshold and leaves
     * {@link Repository#getHydratedAt()} at the generation's publication. A due type whose
     * fingerprint moved, that the answer leaves out, or that a failed rebuild left
     * {@link HydrationState#DEGRADED} is rebuilt, and every type the rebuild covers records the
     * fingerprint it was asked under once the rebuild publishes.
     *
     * <p>A failure is logged with the types it reached. A rebuild that fails leaves each type it
     * covered {@link HydrationState#DEGRADED} on its previous generation and due again at the next
     * tick. A source that cannot be asked leaves every due type unchecked, so one that stays
     * unreachable shows as {@link HydrationState#STALE}. Either way the cadence outlives the failure.
     * An {@link Error} is not caught, and ends the cadence.
     */
    private synchronized void hydrateDue() {
        if (!this.active)
            return;

        ConcurrentList<Class<JpaModel>> due = this.config.models()
            .stream()
            .filter(model -> this.repositories.get(model).isDue())
            .collect(Concurrent.toList());

        if (due.isEmpty())
            return;

        ConcurrentMap<Class<? extends JpaModel>, String> asked;

        try {
            asked = this.config.source().fingerprints(this.covering(due));
        } catch (RuntimeException exception) {
            log.error(
                "A background check could not ask the source about {}, which stay unchecked on their generation",
                due.stream().map(Class::getName).collect(Collectors.joining(", ")),
                exception
            );
            return;
        }

        ConcurrentList<Class<JpaModel>> moved = due.stream()
            .filter(type -> !this.isUnmoved(type, asked))
            .collect(Concurrent.toList());
        ConcurrentList<Class<JpaModel>> rebuilt = this.covering(moved);

        due.stream()
            .filter(type -> !rebuilt.contains(type))
            .forEach(type -> this.repositories.get(type).confirm());

        if (moved.isEmpty())
            return;

        try {
            this.hydrate(moved);
            this.record(asked, rebuilt);
        } catch (RuntimeException exception) {
            log.error(
                "A background rebuild failed and left {} on their previous generation",
                rebuilt.stream().map(Class::getName).collect(Collectors.joining(", ")),
                exception
            );
        }
    }

    /**
     * Whether a due type can keep its held generation: the source answered the fingerprint that
     * generation was read under, and the last rebuild covering the type published.
     *
     * @param type the due type
     * @param asked the fingerprints the source answered for this tick
     * @return {@code true} when the type need not be read
     */
    private boolean isUnmoved(
        @NotNull Class<JpaModel> type,
        @NotNull ConcurrentMap<Class<? extends JpaModel>, String> asked
    ) {
        String fingerprint = asked.get(type);
        HydrationState state = this.repositories.get(type).getState();

        return fingerprint != null
            && fingerprint.equals(this.fingerprints.get(type))
            && (state == HydrationState.CURRENT || state == HydrationState.STALE);
    }

    /**
     * Records the fingerprint each type was read under, forgetting it for a type the answer left
     * out, so a type is only ever skipped against a fingerprint taken before its held rows were
     * read.
     *
     * @param asked the fingerprints the source answered before the read
     * @param types the types the read published
     */
    private void record(
        @NotNull ConcurrentMap<Class<? extends JpaModel>, String> asked,
        @NotNull ConcurrentList<Class<JpaModel>> types
    ) {
        for (Class<JpaModel> type : types) {
            String fingerprint = asked.get(type);

            if (fingerprint == null)
                this.fingerprints.remove(type);
            else
                this.fingerprints.put(type, fingerprint);
        }
    }

    /**
     * Applies one write to the session's source, then rebuilds the written type and every type
     * linking into it.
     *
     * <p>The rebuild is what keeps a held generation honest after a write: the rows the origin now
     * holds are not the rows this session read, and a row linking to the written type would otherwise
     * keep the instances it was linked to before. It is driven by the write rather than by a caller
     * asking for it, and a request naming no rows writes and rebuilds nothing, so nothing downstream
     * gains a way to force a rehydration.
     *
     * <p>The rebuild forgets the fingerprint each type it covered was read under. An origin's
     * catalogue can lag the commit it describes, so an answer the source gives now may still name
     * the rows before the write; forgetting it leaves each of those types to be read again at its
     * next {@link Hydration} tick, whatever its fingerprint says then.
     *
     * <p>A rebuild that fails after the write is not a failure of the write. It is logged with the
     * types it covered, and the write returns. Every one of those types stays
     * {@link HydrationState#DEGRADED}, serving the generation before the write, until a later write
     * or a {@link Hydration} tick covers it, which {@link Repository#getState()} reports. An
     * {@link Error} from the rebuild is not caught.
     *
     * <p>The request names the exact type it writes. A subtype registered in its place is not written
     * through a supertype, because the rows would reach the source under one type and be rebuilt under
     * another.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the session is inactive, the type is not registered exactly, the source
     *         holds no write instruction, or the write fails
     */
    @SuppressWarnings("unchecked")
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.isActive())
            throw new JpaException("Session connection is not active");

        if (!this.config.models().contains(request.type()))
            throw new JpaException("Session holds no '%s' to write it", request.type().getName());

        Class<JpaModel> type = (Class<JpaModel>) (Class<?>) request.type();

        if (!(this.config.source() instanceof Source.Writable writable))
            throw new JpaException("Source for '%s' holds no write instruction", request.type().getName());

        if (request.rows().isEmpty())
            return;

        writable.write(request);

        synchronized (this) {
            ConcurrentList<Class<JpaModel>> written = Concurrent.newList(type);
            ConcurrentList<Class<JpaModel>> covered = this.covering(written);

            try {
                this.hydrate(written);
            } catch (RuntimeException exception) {
                log.error(
                    "A write to '{}' landed, but its rebuild failed and left {} on their previous generation",
                    type.getName(),
                    covered.stream().map(Class::getName).collect(Collectors.joining(", ")),
                    exception
                );
            } finally {
                covered.forEach(this.fingerprints::remove);
            }
        }
    }

    /**
     * Retrieves the {@link Repository} for the given model class: the one registered for the class
     * itself, otherwise the first registered for a subtype of it.
     *
     * @param tClass the entity class to look up
     * @param <M> the entity type
     * @return the matching repository, empty when this session registers none or has been shut down
     */
    @SuppressWarnings("unchecked")
    public <M extends JpaModel> @NotNull Optional<Repository<M>> getRepository(@NotNull Class<M> tClass) {
        if (!this.isActive())
            return Optional.empty();

        return registered(this.config.models(), tClass).map(type -> (Repository<M>) this.repositories.get(type));
    }

    /**
     * Performs an orderly shutdown of this session.
     *
     * <p>Shuts down the scheduler, if one was built, which stops the tick and drops the scheduler's JVM
     * shutdown hook, so nothing the JVM holds keeps this session reachable. It then waits for any
     * rebuild in flight before it marks the session inactive and clears all repositories. The source
     * is not closed: a database stays open for whoever opened it.</p>
     *
     * <p>After shutdown, {@link #getRepository(Class)} answers empty for every type. The session
     * object should be discarded.</p>
     */
    void shutdown() {
        if (this.scheduler != null)
            this.scheduler.shutdown();

        synchronized (this) {
            this.active = false;
            this.repositories.clear();
        }
    }

    /**
     * Maps each registered type to every registered type linking into it, directly or through
     * another.
     *
     * <p>An edge is a {@link Linked} field, or a single-valued JPA association - {@link ManyToOne} or
     * {@link OneToOne} - whose target a registered type answers for. A collection-valued association
     * is not followed. The walk records each dependent once, so a cycle ends with every type on it in
     * the others' sets and adds nothing further.
     *
     * @param models the registered types
     * @return the transitive dependents of every type something links into
     */
    private static @NotNull ConcurrentMap<Class<JpaModel>, ConcurrentSet<Class<JpaModel>>> dependentsOf(
        @NotNull ConcurrentList<Class<JpaModel>> models
    ) {
        // TODO: once the collections pin carries Graph.ancestors, hand this walk to Graph:
        //  - the dependents field becomes `private final @NotNull Graph<Class<JpaModel>> links`,
        //    built in the constructor by linksOf(config.models())
        //  - hydrate asks `covered.addAll(this.links.ancestors(type))` in place of the
        //    dependents.getOrDefault lookup
        //  - this method becomes linksOf, which builds the edges and leaves the closure below to
        //    Graph.ancestors - every node reaching the target along one or more edges, so a type on
        //    a cycle stays in its own set, as it does here
        //  - the ArrayDeque and Deque imports go with the walk
        //
        //    private static @NotNull Graph<Class<JpaModel>> linksOf(@NotNull ConcurrentList<Class<JpaModel>> models) {
        //        return Graph.<Class<JpaModel>>builder()
        //            .withValues(models)
        //            .withEdgeFunction(model -> Stream.concat(
        //                    JpaRepository.links(model).stream(),
        //                    new Reflection<>(model).getFields()
        //                        .stream()
        //                        .filter(field -> field.hasAnnotation(ManyToOne.class) || field.hasAnnotation(OneToOne.class))
        //                )
        //                .map(JpaRepository::targetOf)
        //                .flatMap(target -> registered(models, target).stream()))
        //            .build();
        //    }
        //
        //  JpaSessionRebuildTest pins the transitive and cyclic rebuilds the swap has to keep.
        ConcurrentMap<Class<JpaModel>, ConcurrentSet<Class<JpaModel>>> direct = Concurrent.newMap();

        for (Class<JpaModel> model : models) {
            Stream.concat(
                    JpaRepository.links(model).stream(),
                    new Reflection<>(model).getFields()
                        .stream()
                        .filter(field -> field.hasAnnotation(ManyToOne.class) || field.hasAnnotation(OneToOne.class))
                )
                .map(JpaRepository::targetOf)
                .flatMap(target -> registered(models, target).stream())
                .forEach(target -> direct.computeIfAbsent(target, key -> Concurrent.newSet()).add(model));
        }

        ConcurrentMap<Class<JpaModel>, ConcurrentSet<Class<JpaModel>>> closed = Concurrent.newMap();

        for (Class<JpaModel> target : direct.keySet()) {
            ConcurrentSet<Class<JpaModel>> seen = Concurrent.newSet();
            Deque<Class<JpaModel>> pending = new ArrayDeque<>(direct.get(target));

            while (!pending.isEmpty()) {
                Class<JpaModel> dependent = pending.pop();

                if (seen.add(dependent))
                    pending.addAll(direct.getOrDefault(dependent, Concurrent.newSet()));
            }

            closed.put(target, seen);
        }

        return closed;
    }

    /**
     * Finds the registered type that answers for a class: the class itself when it is registered,
     * otherwise the first registered type it is assignable from.
     *
     * @param models the registered types, in registration order
     * @param type the class asked about
     * @return the registered type, empty when none answers
     */
    @SuppressWarnings("unchecked")
    private static @NotNull Optional<Class<JpaModel>> registered(
        @NotNull ConcurrentList<Class<JpaModel>> models,
        @NotNull Class<?> type
    ) {
        if (models.contains(type))
            return Optional.of((Class<JpaModel>) type);

        return models.stream()
            .filter(type::isAssignableFrom)
            .findFirst();
    }

}

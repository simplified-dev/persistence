package dev.simplified.persistence;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Log;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.ConcurrentSet;
import dev.simplified.collection.sort.Graph;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import dev.simplified.scheduler.Scheduler;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.FetchType;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
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
 * <p>A registered type declaring a collection-valued association or an element collection -
 * {@link OneToMany}, {@link ManyToMany}, {@link ElementCollection} - or a lazy single-valued
 * association is refused at connect, before anything is read. Read from a database, a lazy field
 * fails once the read that loaded its owner has closed, and a rebuild does not follow a collection,
 * so none of them can be served from a held generation. Such a type belongs outside the registered
 * models, reached through the database that maps it. An eager single-valued association reads its
 * target with its owner whether or not the target is registered, so the check follows every one of
 * them out of a registered type, through as many unregistered types as they lead to, and refuses
 * the same fields on each type it reaches, naming the path from the registered type.
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
     * The registered types, with an edge from each one to every registered type it links into.
     */
    private final @NotNull Graph<Class<JpaModel>> links;

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
     * @throws JpaException if a registered type, or a type one reaches through eager single-valued
     *         associations, declares a collection-valued, element-collection or lazy association, or a
     *         link or association naming no model it can resolve to
     */
    JpaSession(@NotNull JpaConfig config) {
        this.config = config;
        this.links = linksOf(config.models());
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
     * linking into one of them, directly or through another registered type.
     *
     * @param asked the registered types asked to rebuild
     * @return the covered types, in registration order
     */
    private @NotNull ConcurrentList<Class<JpaModel>> covering(@NotNull ConcurrentList<Class<JpaModel>> asked) {
        ConcurrentSet<Class<JpaModel>> covered = Concurrent.newSet();

        asked.forEach(type -> {
            covered.add(type);
            covered.addAll(this.links.ancestors(type));
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
     * <p>An upsert is checked before it is written. Its rows are linked against the rows this session
     * holds, with the request's own rows keyed over the written type's, so a row linking to one the
     * same request adds resolves. A row whose link is neither a list nor an {@link Optional} and
     * carries no id or names no row refuses the whole write before anything reaches the source. The
     * check fills in the request rows' {@link Linked} fields, which serialization skips. A request
     * writes one type and every other type answers its held rows, so two new rows of different types
     * naming each other through plain links cannot be written - whichever goes first names a row
     * nothing holds yet - and one side has to link through an {@link Optional}, or first name a row
     * that is already held. A delete is not checked: removing a row that other rows still name lands,
     * then fails its own rebuild and every later connect until the data is repaired.
     *
     * <p>The request names the exact type it writes. A subtype registered in its place is not written
     * through a supertype, because the rows would reach the source under one type and be rebuilt under
     * another.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the session is inactive, the type is not registered exactly, the source
     *         holds no write instruction, an upserted row's link that is neither a list nor an
     *         {@link Optional} carries no id or names no row, or the write fails
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

        if (request.operation() == WriteRequest.Operation.UPSERT) {
            synchronized (this) {
                if (!this.active)
                    throw new JpaException("Session connection is not active");

                ConcurrentList<JpaModel> rows = (ConcurrentList<JpaModel>) (ConcurrentList<?>) request.rows();

                this.repositories.get(type).link(rows, target -> {
                    ConcurrentMap<String, JpaModel> keyed = this.lookupFor(target, Concurrent.newMap());

                    if (registered(this.config.models(), target).filter(type::equals).isPresent())
                        keyed.putAll(JpaModel.keyed(type, rows));

                    return keyed;
                });
            }
        }

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
     * Builds the graph of links between the registered types, refusing a type that declares, or
     * reaches a type declaring, a field a held generation cannot follow.
     *
     * <p>An edge runs from a registered type to the registered type answering for the target of one
     * of its fields: a {@link Linked} field, or a single-valued JPA association - {@link ManyToOne} or
     * {@link OneToOne}. A field declaring {@link OneToMany}, {@link ManyToMany} or
     * {@link ElementCollection}, or a single-valued association fetched {@link FetchType#LAZY}, is
     * refused rather than followed or skipped, on a registered type and on every unregistered type
     * its eager associations reach. Every type is checked while the graph is built, so a refusal is
     * thrown before anything is read.
     *
     * <p>A cycle is not refused. {@link Graph#ancestors} answers over one, so a rebuild of any type
     * on it covers every other.
     *
     * @param models the registered types
     * @return the links, with an edge from each registered type to every registered type it links
     *         into
     * @throws JpaException if a registered type, or a type one reaches through eager single-valued
     *         associations, declares a collection-valued, element-collection or lazy association, or a
     *         link or association naming no model it can resolve to
     */
    private static @NotNull Graph<Class<JpaModel>> linksOf(@NotNull ConcurrentList<Class<JpaModel>> models) {
        return Graph.<Class<JpaModel>>builder()
            .withValues(models)
            .withEdgeFunction(model -> {
                ConcurrentSet<FieldAccessor<?>> fields = new Reflection<>(model).getFields();
                refuseUnfollowable(models, model, model, "", Concurrent.newSet());

                return Stream.concat(
                        JpaRepository.links(model).stream(),
                        fields.stream().filter(field -> field.hasAnnotation(ManyToOne.class) || field.hasAnnotation(OneToOne.class))
                    )
                    .map(JpaRepository::targetOf)
                    .flatMap(target -> registered(models, target).stream());
            })
            .build();
    }

    /**
     * Refuses a type declaring a field a held generation cannot follow, then walks on through each of
     * its single-valued associations into every unregistered type they reach.
     *
     * <p>An association this check leaves standing is eager, and reads its target with its owner
     * whether or not the target is registered, so a lazy field there fails once the read has closed
     * just as one on the registered type would. The walk stops at a registered type, which is checked
     * as itself, and at a type it has already reached, so a cycle ends.
     *
     * @param models the registered types
     * @param model the registered type the walk starts from
     * @param type the type to check, {@code model} itself where the walk starts
     * @param path the fields leading from {@code model} to {@code type}, joined by dots, empty where
     *        the walk starts
     * @param reached the unregistered types the walk has already checked
     * @throws JpaException if the type, or one it reaches, declares a collection-valued,
     *         element-collection or lazy association, or an association naming no model it can
     *         resolve to
     */
    private static void refuseUnfollowable(
        @NotNull ConcurrentList<Class<JpaModel>> models,
        @NotNull Class<JpaModel> model,
        @NotNull Class<? extends JpaModel> type,
        @NotNull String path,
        @NotNull ConcurrentSet<Class<?>> reached
    ) {
        ConcurrentSet<FieldAccessor<?>> fields = new Reflection<>(type).getFields();

        for (FieldAccessor<?> field : fields) {
            String unfollowable = field.hasAnnotation(OneToMany.class) ? "@OneToMany"
                : field.hasAnnotation(ManyToMany.class) ? "@ManyToMany"
                : field.hasAnnotation(ElementCollection.class) ? "@ElementCollection"
                : field.getAnnotation(ManyToOne.class).filter(association -> association.fetch() == FetchType.LAZY).isPresent() ? "a lazy @ManyToOne"
                : field.getAnnotation(OneToOne.class).filter(association -> association.fetch() == FetchType.LAZY).isPresent() ? "a lazy @OneToOne"
                : null;

            if (unfollowable == null)
                continue;

            if (path.isEmpty())
                throw new JpaException("Field '%s' of '%s' declares %s, which a held generation cannot follow", field.getName(), model.getName(), unfollowable);

            throw new JpaException(
                "Field '%s' of '%s', reached from '%s' through '%s', declares %s, which a held generation cannot follow",
                field.getName(),
                type.getName(),
                model.getName(),
                path,
                unfollowable
            );
        }

        for (FieldAccessor<?> field : fields) {
            if (!field.hasAnnotation(ManyToOne.class) && !field.hasAnnotation(OneToOne.class))
                continue;

            Class<? extends JpaModel> target = JpaRepository.targetOf(field);

            if (!models.contains(target) && reached.add(target))
                refuseUnfollowable(models, model, target, path.isEmpty() ? field.getName() : path + "." + field.getName(), reached);
        }
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

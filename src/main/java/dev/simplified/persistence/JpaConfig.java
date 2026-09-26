package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.gson.PostInit;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.RelationalSource;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.persistence.Cacheable;
import org.hibernate.annotations.Cache;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Optional;

/**
 * What a session holds: the models it registers and the one source every one of them is read from.
 *
 * <p>A session never asks what kind of source it was given. A database is opened by the caller before
 * it is handed in - through the {@link RelationalSource.Builder} its driver answers - and kept by the
 * caller for Hibernate access. Closing it is optional: the JVM closes one still open at exit, and
 * closing it explicitly releases it earlier, once every session reading it is shut down, because a
 * session reading a closed database fails its next write, rebuild or tick. A document source is
 * neither opened nor closed.
 *
 * <p>Registration is the whole of the choice a type makes. A type in {@link #models()} holds a
 * generation the session hydrates; a relational type left out of it is still reached through the
 * {@link RelationalSource} the caller opened, provided that database maps it. A registered type is
 * written through {@link JpaSession#write}, which checks the write and rebuilds what it changed, or
 * by a caller holding no session through {@link #write(WriteRequest)}, which checks it the same way
 * and rebuilds nothing. A write straight through {@link #source()} is not checked, and leaves a
 * session's held rows as they were.
 *
 * <p>What registering costs is memory and rebuilds. The session holds every row of a registered type
 * in memory, and re-reads it, with every registered type linking into it, after each write through
 * the session and at each {@link Hydration} tick it comes due at, unless the source's fingerprint
 * shows it has not moved. A relational type left out is read per query instead, through the
 * database's Hibernate access. There the second-level cache serves a lookup by id only for a type
 * declared {@link Cacheable} or {@link Cache}, and a query result only when the query cache is on and
 * the query is marked cacheable.
 *
 * @param models the model classes the session holds a repository for, typically discovered through
 *        {@link JpaModel#resolveModels(Class)}
 * @param source where every registered type's rows are read from, and written to when it is a
 *        {@link Source.Writable}
 * @see SessionManager#connect(JpaConfig)
 */
public record JpaConfig(@NotNull ConcurrentList<Class<JpaModel>> models, @NotNull Source source) {

    /**
     * Applies one write to the source, checked the way {@link JpaSession#write(WriteRequest)} checks
     * it, for a caller holding no session.
     *
     * <p>Only a {@link Linked} field that is neither a list nor an {@link Optional} has no way to hold
     * a miss, so only such a link is checked. An upsert is refused whole when one of its rows' such
     * links carries no id, names no row - the request's own rows counting as rows of the written
     * type - or links into a class no registered type answers for. A delete is refused whole when a
     * row it leaves names one of the deleted rows through such a link, so rows naming only each other
     * can be deleted together when they are of the written type; two rows of different types naming
     * each other through such links cannot be deleted, since whichever goes first is still named by
     * the other. Either refusal comes before anything reaches the source.
     *
     * <p>The check reads each type it needs from the source the way a session reads a type it
     * hydrates, {@link PostInit} included, so it sees the id properties a rebuild would link through.
     * It reads at the moment of the write, so a commit another writer lands between the check's read
     * and the write is not seen.
     *
     * <p>Nothing is rebuilt, so a session reading the same source keeps serving the rows it read until
     * a rebuild of its own. A request naming no rows writes nothing.
     *
     * @param request the write to apply
     * @param <M> the entity type
     * @throws JpaException if the type is not registered exactly, the source holds no write
     *         instruction, an upserted row's link that is neither a list nor an {@link Optional}
     *         carries no id, names no row or links into a class no registered type answers for, a
     *         row the delete leaves names a deleted row through such a link, a read the check needs
     *         fails, or the write fails
     */
    public <M extends JpaModel> void write(@NotNull WriteRequest<M> request) {
        if (!this.models.contains(request.type()))
            throw new JpaException("Config registers no '%s' to write it", request.type().getName());

        if (!(this.source instanceof Source.Writable writable))
            throw new JpaException("Source for '%s' holds no write instruction", request.type().getName());

        if (request.rows().isEmpty())
            return;

        JpaRepository.refuseDangling(this.models, request, model -> new JpaRepository<>(model, Duration.ZERO).hydrate(this.source));
        writable.write(request);
    }

}

package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.source.RelationalOrigin;
import dev.simplified.persistence.source.RelationalSource;
import dev.simplified.persistence.source.Source;
import org.jetbrains.annotations.NotNull;

/**
 * What a session holds: the models it registers and the one source every one of them is read from.
 *
 * <p>A session never asks what kind of source it was given. A database is opened by the caller before
 * it is handed in - through {@link RelationalOrigin#open} - kept by the caller for Hibernate access,
 * and closed by the caller once the session is shut down. A document source has neither step.
 *
 * <p>Registration is the whole of the choice a type makes. A type in {@link #models()} holds a
 * generation the session hydrates; a relational type left out of it is still reached through the
 * {@link RelationalSource} the caller opened, provided that database maps it.
 *
 * @param models the model classes the session holds a repository for, typically discovered through
 *        {@link JpaModel#resolveModels(Class)}
 * @param source where every registered type's rows are read from, and written to when it is a
 *        {@link Source.Writable}
 * @see SessionManager#connect(JpaConfig)
 */
public record JpaConfig(@NotNull ConcurrentList<Class<JpaModel>> models, @NotNull Source source) {}

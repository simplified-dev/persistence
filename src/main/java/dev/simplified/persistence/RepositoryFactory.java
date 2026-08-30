package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.store.RelationalOrigin;
import dev.simplified.persistence.store.Source;
import dev.simplified.reflection.Reflection;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Optional;

/**
 * What a session needs to build its repositories: the types it holds and where their rows come from.
 *
 * <p>Two members, because a {@link Source} serves every type an origin publishes. A factory that
 * draws from more than one origin overrides {@link #sourceFor(Class)}.
 *
 * <p>An empty source is not a factory holding nothing - it says the rows are the database's, and the
 * session substitutes the {@link RelationalOrigin} it opened. A factory that names an origin and a
 * session that holds a database are two answers to one question, and the factory's wins.
 *
 * <pre>{@code
 * public class CorpusFactory implements RepositoryFactory {
 *
 *     @Getter private final ConcurrentList<Class<JpaModel>> models = RepositoryFactory.resolveModels(Item.class);
 *     @Getter private final Optional<Source> source = Optional.of(Source.documents(origin, gson));
 *
 * }
 * }</pre>
 *
 * @see JpaConfig.Builder#withRepositoryFactory(RepositoryFactory)
 */
public interface RepositoryFactory {

    /**
     * The entity classes this factory holds repositories for.
     */
    @NotNull ConcurrentList<Class<JpaModel>> getModels();

    /**
     * Where the rows for every type come from, empty when they come from the session's database.
     */
    @NotNull Optional<Source> getSource();

    /**
     * Where one type's rows come from, for a factory drawing on more than one origin.
     *
     * @param type the entity class
     * @return the source that holds it, empty when the session's database does
     */
    default @NotNull Optional<Source> sourceFor(@NotNull Class<? extends JpaModel> type) {
        return this.getSource();
    }

    /**
     * Returns a factory over every {@link JpaModel} under the anchor's package, whose rows the
     * session's database authors.
     *
     * @param anchor the class whose package scopes the scan
     * @return a factory naming no origin of its own
     */
    static @NotNull RepositoryFactory of(@NotNull Class<? extends JpaModel> anchor) {
        return of(anchor, Optional.empty());
    }

    /**
     * Returns a factory over every {@link JpaModel} under the anchor's package, reading from the given
     * origin.
     *
     * @param anchor the class whose package scopes the scan
     * @param source where the rows come from
     * @return a factory over the discovered types
     */
    static @NotNull RepositoryFactory of(@NotNull Class<? extends JpaModel> anchor, @NotNull Source source) {
        return of(anchor, Optional.of(source));
    }

    /**
     * Returns a factory over every {@link JpaModel} under the anchor's package.
     *
     * @param anchor the class whose package scopes the scan
     * @param source where the rows come from, empty when the session's database holds them
     * @return a factory over the discovered types
     */
    private static @NotNull RepositoryFactory of(@NotNull Class<? extends JpaModel> anchor, @NotNull Optional<Source> source) {
        ConcurrentList<Class<JpaModel>> models = resolveModels(anchor);

        return new RepositoryFactory() {

            @Override
            public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
                return models;
            }

            @Override
            public @NotNull Optional<Source> getSource() {
                return source;
            }

        };
    }

    /**
     * Discovers all {@link JpaModel} implementations via classpath scanning scoped to the given
     * anchor class's package, in a stable order.
     *
     * <p>The order is by class name and carries no meaning. A hydration reads every type and only
     * then links every type, so nothing needs a parent to precede its children and there is no
     * dependency graph to sort - which is also what stops two types that reach each other from
     * reading as a cycle.
     *
     * @param anchor the class whose package scopes the scan
     * @return the discovered entity classes, ordered by name
     */
    static @NotNull ConcurrentList<Class<JpaModel>> resolveModels(@NotNull Class<? extends JpaModel> anchor) {
        return Reflection.getResources()
            .filterPackage(anchor)
            .getTypesOf(JpaModel.class)
            .stream()
            .sorted(Comparator.comparing(Class::getName))
            .collect(Concurrent.toUnmodifiableList());
    }

}

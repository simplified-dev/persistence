package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.store.Source;
import dev.simplified.reflection.Reflection;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;

/**
 * What a session needs to build its repositories: the types it holds and where their rows come from.
 *
 * <p>Two members, because a {@link Source} serves every type an origin publishes. A factory that
 * draws from more than one origin overrides {@link #sourceFor(Class)}.
 *
 * <pre>{@code
 * public class CorpusFactory implements RepositoryFactory {
 *
 *     @Getter private final ConcurrentList<Class<JpaModel>> models = RepositoryFactory.resolveModels(Item.class);
 *     @Getter private final Source source = Source.documents(origin, gson);
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
     * Where the rows for every type come from.
     */
    @NotNull Source getSource();

    /**
     * Where one type's rows come from, for a factory drawing on more than one origin.
     *
     * @param type the entity class
     * @return the source that holds it
     */
    default @NotNull Source sourceFor(@NotNull Class<? extends JpaModel> type) {
        return this.getSource();
    }

    /**
     * Returns a factory over every {@link JpaModel} under the anchor's package, whose rows the
     * database itself authors.
     *
     * @param anchor the class whose package scopes the scan
     * @return a factory holding no external origin
     */
    static @NotNull RepositoryFactory of(@NotNull Class<? extends JpaModel> anchor) {
        return of(anchor, Source.none());
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
        ConcurrentList<Class<JpaModel>> models = resolveModels(anchor);

        return new RepositoryFactory() {

            @Override
            public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
                return models;
            }

            @Override
            public @NotNull Source getSource() {
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

package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.sort.Graph;
import dev.simplified.persistence.store.Source;
import dev.simplified.reflection.Reflection;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * What a session needs to build its repositories: the types it holds and where their rows come from.
 *
 * <p>Two members, because a {@link Source} serves every type an origin publishes. A factory that
 * draws from more than one origin overrides {@link #sourceFor(Class)}.
 *
 * <pre>{@code
 * public class SkyBlockFactory implements RepositoryFactory {
 *
 *     @Getter private final ConcurrentList<Class<JpaModel>> models = RepositoryFactory.resolveModels(Item.class);
 *     @Getter private final Source source = CORPUS.reading();
 *
 * }
 * }</pre>
 *
 * @see JpaConfig.Builder#withRepositoryFactory(RepositoryFactory)
 */
public interface RepositoryFactory {

    /**
     * Topologically sorted entity classes this factory holds repositories for.
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
     * Discovers all {@link JpaModel} implementations via classpath scanning scoped to
     * the given anchor class's package and returns them topologically sorted by
     * inter-entity dependencies.
     *
     * <p>Dependencies are inferred from declared fields: direct {@link JpaModel} field types
     * and {@link JpaModel} type arguments of parameterized fields (e.g.
     * {@code ConcurrentList<SomeModel>}) are treated as edges in the dependency graph.
     * {@linkplain #isInverseSide(Field) Inverse sides} of bidirectional associations are excluded,
     * because the entity that owns the foreign key is the one that constrains ordering.
     *
     * @param anchor the class whose package scopes the scan
     * @return a topologically sorted list of discovered entity classes
     * @throws IllegalStateException if the entities depend on each other cyclically
     */
    @SuppressWarnings("unchecked")
    static @NotNull ConcurrentList<Class<JpaModel>> resolveModels(@NotNull Class<? extends JpaModel> anchor) {
        return Graph.<Class<JpaModel>>builder()
            .withValues(
                Reflection.getResources()
                    .filterPackage(anchor)
                    .getTypesOf(JpaModel.class)
            )
            .withEdgeFunction(type -> Arrays.stream(type.getDeclaredFields())
                .filter(field -> !isInverseSide(field))
                .flatMap(field -> {
                    Type genericType = field.getGenericType();

                    if (genericType instanceof ParameterizedType pt)
                        return Arrays.stream(pt.getActualTypeArguments())
                            .filter(arg -> arg instanceof Class && JpaModel.class.isAssignableFrom((Class<?>) arg))
                            .map(arg -> (Class<JpaModel>) arg);

                    Class<?> fieldType = field.getType();

                    if (JpaModel.class.isAssignableFrom(fieldType))
                        return Stream.of((Class<JpaModel>) fieldType);

                    return Stream.empty();
                })
            )
            .build()
            .linearTopologicalSort();
    }

    /**
     * Returns whether a field is the inverse side of a bidirectional association.
     *
     * <p>An inverse side names the field that owns the association through {@code mappedBy}, so the
     * foreign key belongs to the other entity and so does the only ordering constraint. Reading it as
     * a dependency asserts the reverse of the truth - the owning side already contributes the correct
     * edge - and the two together read as a cycle.
     *
     * <p>A relationship without {@code mappedBy} is an owning side and is left alone, including a
     * unidirectional {@code @OneToMany}.
     *
     * @param field the declared field to test
     * @return {@code true} when another entity owns the association
     */
    private static boolean isInverseSide(@NotNull Field field) {
        OneToMany oneToMany = field.getAnnotation(OneToMany.class);

        if (oneToMany != null)
            return !oneToMany.mappedBy().isEmpty();

        OneToOne oneToOne = field.getAnnotation(OneToOne.class);

        if (oneToOne != null)
            return !oneToOne.mappedBy().isEmpty();

        ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);

        return manyToMany != null && !manyToMany.mappedBy().isEmpty();
    }

}

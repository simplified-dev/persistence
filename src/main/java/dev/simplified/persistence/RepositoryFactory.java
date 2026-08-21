package dev.simplified.persistence;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.sort.Graph;
import dev.simplified.persistence.store.EntityStore;
import dev.simplified.reflection.Reflection;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Factory for creating {@link JpaRepository} instances during {@link JpaSession#cacheRepositories()},
 * holding the discovered model list and per-type {@link EntityStore} registrations.
 *
 * <p>Provides static utilities ({@link #resolveModels(Class)}) and default method implementations
 * ({@link #create(JpaSession, Class)}) so that custom implementations only need to supply
 * configuration data. The {@link Builder} and {@link Impl} pair remain as a convenience
 * shortcut for inline construction.
 *
 * @see JpaConfig.Builder#withRepositoryFactory(RepositoryFactory)
 * @see Impl
 * @see Builder
 */
public interface RepositoryFactory {

    /**
     * Topologically sorted entity classes discovered for this factory.
     */
    @NotNull ConcurrentList<Class<JpaModel>> getModels();

    /**
     * The fallback store for entity types without an explicit per-type registration.
     * Returns {@code null} for SQL-managed entities that require no external data loading.
     */
    default @Nullable EntityStore<?> getDefaultStore() {
        return null;
    }

    /**
     * Per-type store registrations, keyed by entity class.
     */
    default @NotNull ConcurrentMap<Class<?>, EntityStore<?>> getStores() {
        return Concurrent.newUnmodifiableMap();
    }

    /**
     * Per-type stream peek consumers, keyed by entity class.
     */
    default @NotNull ConcurrentMap<Class<?>, Consumer<?>> getPeeks() {
        return Concurrent.newUnmodifiableMap();
    }

    /**
     * Creates a repository for the given entity type within the given session.
     *
     * <p>Resolves the {@link EntityStore} from {@link #getStores()}, falling back
     * to {@link #getDefaultStore()}, and the optional stream peek from {@link #getPeeks()}.
     *
     * @param session the JPA session that will own the repository
     * @param type the entity class
     * @param <T> the entity type
     * @return a new repository instance
     */
    @SuppressWarnings("unchecked")
    default <T extends JpaModel> @NotNull JpaRepository<T> create(@NotNull JpaSession session, @NotNull Class<T> type) {
        EntityStore<T> store = (EntityStore<T>) this.getStores().getOrDefault(type, this.getDefaultStore());
        Consumer<T> peek = (Consumer<T>) this.getPeeks().get(type);
        return new JpaRepository<>(session, type, store, peek);
    }

    /**
     * Returns a new {@link Builder} for constructing the default {@link Impl}.
     *
     * @return a new builder
     */
    static @NotNull Builder builder() {
        return new Builder();
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

    /**
     * Fluent builder for constructing the default {@link Impl} with per-type
     * {@link EntityStore} and stream peek registrations.
     *
     * <p>Types without an explicit registration fall back to the default store
     * (which defaults to {@code null} - SQL-managed - if not set).
     */
    class Builder {

        private @NotNull Class<? extends JpaModel> packageAnchor = JpaModel.class;
        private @Nullable EntityStore<?> defaultStore;
        private final @NotNull ConcurrentMap<Class<?>, EntityStore<?>> stores = Concurrent.newMap();
        private final @NotNull ConcurrentMap<Class<?>, Consumer<?>> peeks = Concurrent.newMap();

        /**
         * Sets the anchor class whose package scopes the classpath scan for
         * {@link JpaModel} entities.
         *
         * @param anchor the anchor class
         * @return this builder
         */
        public @NotNull Builder withPackageOf(@NotNull Class<? extends JpaModel> anchor) {
            this.packageAnchor = anchor;
            return this;
        }

        /**
         * Sets the default {@link EntityStore} for types without an explicit registration.
         *
         * @param store the default store
         * @return this builder
         */
        public @NotNull Builder withDefault(@NotNull EntityStore<?> store) {
            this.defaultStore = store;
            return this;
        }

        /**
         * Registers a {@link EntityStore} for a specific entity type.
         *
         * @param type the entity class
         * @param store the store for this type
         * @param <T> the entity type
         * @return this builder
         */
        public <T extends JpaModel> @NotNull Builder with(@NotNull Class<T> type, @NotNull EntityStore<T> store) {
            this.stores.put(type, store);
            return this;
        }

        /**
         * Registers a {@link EntityStore} and stream peek for a specific entity type.
         *
         * @param type the entity class
         * @param store the store for this type
         * @param peek the per-entity consumer applied on every query
         * @param <T> the entity type
         * @return this builder
         */
        public <T extends JpaModel> @NotNull Builder with(@NotNull Class<T> type, @NotNull EntityStore<T> store, @NotNull Consumer<T> peek) {
            this.stores.put(type, store);
            this.peeks.put(type, peek);
            return this;
        }

        /**
         * Builds the default {@link Impl}.
         *
         * <p>Discovers all {@link JpaModel} implementations via classpath scanning scoped
         * to the configured {@link #withPackageOf(Class) package anchor}, topologically
         * sorts them by inter-entity dependencies, then constructs the factory with per-type
         * registrations. Types without an explicit registration fall back to the default
         * source; if no default is set, {@code null} is used (SQL-managed).
         *
         * @return a new repository factory
         */
        public @NotNull RepositoryFactory build() {
            return new Impl(
                RepositoryFactory.resolveModels(this.packageAnchor),
                this.defaultStore,
                this.stores.toUnmodifiable(),
                this.peeks.toUnmodifiable()
            );
        }

    }

    /**
     * Default implementation of {@link RepositoryFactory} backed by per-type
     * {@link EntityStore} registrations and optional stream peek consumers.
     *
     * <p>Constructed exclusively via {@link RepositoryFactory#builder()}.
     *
     * @see Builder
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    class Impl implements RepositoryFactory {

        /**
         * Topologically sorted entity classes discovered from the configured package anchor.
         */
        private final @NotNull ConcurrentList<Class<JpaModel>> models;

        /**
         * The fallback store for types without an explicit registration.
         */
        private final @Nullable EntityStore<?> defaultStore;

        /**
         * Per-type store registrations.
         */
        private final @NotNull ConcurrentMap<Class<?>, EntityStore<?>> stores;

        /**
         * Per-type stream peek consumers.
         */
        private final @NotNull ConcurrentMap<Class<?>, Consumer<?>> peeks;

    }

}

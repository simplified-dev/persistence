package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.collection.sort.Graph;
import dev.simplified.persistence.source.Source;
import dev.simplified.reflection.Reflection;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Factory for creating {@link JpaRepository} instances during {@link JpaSession#cacheRepositories()},
 * holding the discovered model list and per-type {@link Source} registrations.
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
     * The fallback source for entity types without an explicit per-type registration.
     * Returns {@code null} for SQL-managed entities that require no external data loading.
     */
    default @Nullable Source<?> getDefaultSource() {
        return null;
    }

    /**
     * Per-type source registrations, keyed by entity class.
     */
    default @NotNull ConcurrentMap<Class<?>, Source<?>> getSources() {
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
     * <p>Resolves the {@link Source} from {@link #getSources()}, falling back
     * to {@link #getDefaultSource()}, and the optional stream peek from {@link #getPeeks()}.
     *
     * @param session the JPA session that will own the repository
     * @param type the entity class
     * @param <T> the entity type
     * @return a new repository instance
     */
    @SuppressWarnings("unchecked")
    default <T extends JpaModel> @NotNull JpaRepository<T> create(@NotNull JpaSession session, @NotNull Class<T> type) {
        Source<T> source = (Source<T>) this.getSources().getOrDefault(type, this.getDefaultSource());
        Consumer<T> peek = (Consumer<T>) this.getPeeks().get(type);
        return new JpaRepository<>(session, type, source, peek);
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
     *
     * @param anchor the class whose package scopes the scan
     * @return a topologically sorted list of discovered entity classes
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
            .topologicalSort();
    }

    /**
     * Fluent builder for constructing the default {@link Impl} with per-type
     * {@link Source} and stream peek registrations.
     *
     * <p>Types without an explicit registration fall back to the default source
     * (which defaults to {@code null} - SQL-managed - if not set).
     */
    class Builder {

        private @NotNull Class<? extends JpaModel> packageAnchor = JpaModel.class;
        private @Nullable Source<?> defaultSource;
        private final @NotNull ConcurrentMap<Class<?>, Source<?>> sources = Concurrent.newMap();
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
         * Sets the default {@link Source} for types without an explicit registration.
         *
         * @param source the default source
         * @return this builder
         */
        public @NotNull Builder withDefault(@NotNull Source<?> source) {
            this.defaultSource = source;
            return this;
        }

        /**
         * Registers a {@link Source} for a specific entity type.
         *
         * @param type the entity class
         * @param source the source for this type
         * @param <T> the entity type
         * @return this builder
         */
        public <T extends JpaModel> @NotNull Builder with(@NotNull Class<T> type, @NotNull Source<T> source) {
            this.sources.put(type, source);
            return this;
        }

        /**
         * Registers a {@link Source} and stream peek for a specific entity type.
         *
         * @param type the entity class
         * @param source the source for this type
         * @param peek the per-entity consumer applied on every query
         * @param <T> the entity type
         * @return this builder
         */
        public <T extends JpaModel> @NotNull Builder with(@NotNull Class<T> type, @NotNull Source<T> source, @NotNull Consumer<T> peek) {
            this.sources.put(type, source);
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
                this.defaultSource,
                this.sources.toUnmodifiable(),
                this.peeks.toUnmodifiable()
            );
        }

    }

    /**
     * Default implementation of {@link RepositoryFactory} backed by per-type
     * {@link Source} registrations and optional stream peek consumers.
     *
     * <p>Constructed exclusively via {@link RepositoryFactory#builder()}.
     *
     * @see Builder
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    class Impl implements RepositoryFactory {

        /** Topologically sorted entity classes discovered from the configured package anchor. */
        private final @NotNull ConcurrentList<Class<JpaModel>> models;

        /** The fallback source for types without an explicit registration. */
        private final @Nullable Source<?> defaultSource;

        /** Per-type source registrations. */
        private final @NotNull ConcurrentMap<Class<?>, Source<?>> sources;

        /** Per-type stream peek consumers. */
        private final @NotNull ConcurrentMap<Class<?>, Consumer<?>> peeks;

    }

}

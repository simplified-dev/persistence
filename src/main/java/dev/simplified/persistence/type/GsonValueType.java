package dev.simplified.persistence.type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.mapping.BasicValue;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Hibernate {@link UserType} that stores one column through a {@link Codec}.
 *
 * <p>The user type is the same for every column - equality, mutability and the copy-on-replace
 * contract do not vary with what the column holds. What varies is how a value crosses the JDBC
 * boundary, and that is the codec's whole job.
 *
 * <p>Four codecs cover every shape the entity models declare: a {@link GsonType @GsonType} class
 * held directly, a {@code List<E>}, a {@code Map<K, V>} and an {@code Optional<I>}. All but the
 * optional store JSON in a CLOB; the optional gives the eight wrapper types a native SQL type and
 * bypasses Gson for them.
 *
 * <p>Registration is automatic through {@link Registrar}, which discovers the shapes in entity
 * fields, registers an instance per parameterisation, and re-points each property at its own
 * instance once the metadata is built.
 *
 * @param <T> the value one column holds
 */
public final class GsonValueType<T> implements UserType<T> {

    /**
     * The inner optional types stored as themselves rather than as JSON, and the SQL type each
     * binds as.
     */
    private static final Map<Class<?>, Integer> NATIVE_SQL_TYPES = Map.of(
        String.class, Types.VARCHAR,
        Boolean.class, Types.BOOLEAN,
        Integer.class, Types.INTEGER,
        Long.class, Types.BIGINT,
        Short.class, Types.SMALLINT,
        Byte.class, Types.TINYINT,
        Double.class, Types.DOUBLE,
        Float.class, Types.FLOAT
    );

    private final @NotNull Codec<T> codec;

    /**
     * Constructs a user type backed by the given codec.
     *
     * @param codec how one column value crosses the JDBC boundary
     */
    public GsonValueType(@NotNull Codec<T> codec) {
        this.codec = codec;
    }

    /** {@inheritDoc} */
    @Override
    public int getSqlType() {
        return this.codec.sqlType();
    }

    /** {@inheritDoc} */
    @Override
    public Class<T> returnedClass() {
        return this.codec.returnedClass();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(T x, T y) {
        return Objects.equals(x, y);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode(T x) {
        return Objects.hashCode(x);
    }

    /** {@inheritDoc} */
    @Override
    public T nullSafeGet(@NotNull ResultSet rs, int position, WrapperOptions options) throws SQLException {
        return this.codec.read(rs, position);
    }

    /** {@inheritDoc} */
    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, T value, int index, WrapperOptions options) throws SQLException {
        this.codec.write(st, value, index);
    }

    /** {@inheritDoc} */
    @Override
    public T deepCopy(T value) {
        return this.codec.copy(value);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public Serializable disassemble(T value) {
        return this.codec.disassemble(value);
    }

    /** {@inheritDoc} */
    @Override
    public T assemble(Serializable cached, Object owner) {
        return this.codec.assemble(cached);
    }

    /** {@inheritDoc} */
    @Override
    public T replace(T original, T target, Object owner) {
        return this.codec.copy(original);
    }

    /**
     * Returns a codec for a class stored as a JSON CLOB.
     *
     * @param gson the Gson instance for serialization
     * @param type the stored class
     * @param <T> the stored type
     * @return a codec for that class
     */
    static <T> @NotNull Codec<T> scalar(@NotNull Gson gson, @NotNull Class<T> type) {
        return new ScalarCodec<>(gson, type);
    }

    /**
     * Returns a codec for a list stored as a JSON CLOB.
     *
     * <p>The read targets an {@code E[]} rather than a {@code List<E>} so Gson keeps the concrete
     * element class - read as a list, every JSON number would come back a {@code Double}.
     *
     * @param gson the Gson instance for serialization
     * @param elementType the concrete element class
     * @param <E> the element type
     * @return a codec for a list of that element
     */
    @SuppressWarnings("unchecked")
    static <E> @NotNull Codec<List<E>> list(@NotNull Gson gson, @NotNull Class<E> elementType) {
        return new ListCodec<>(gson, (Class<E[]>) Array.newInstance(elementType, 0).getClass());
    }

    /**
     * Returns a codec for a map stored as a JSON CLOB, read with full generic type information.
     *
     * @param gson the Gson instance for serialization
     * @param keyType the map key class
     * @param valueType the map value class
     * @param <K> the key type
     * @param <V> the value type
     * @return a codec for a map of that key and value
     */
    @SuppressWarnings("unchecked")
    static <K, V> @NotNull Codec<Map<K, V>> map(@NotNull Gson gson, @NotNull Class<K> keyType, @NotNull Class<V> valueType) {
        return (Codec<Map<K, V>>) (Codec<?>) new MapCodec(gson, TypeToken.getParameterized(ConcurrentMap.class, keyType, valueType).getType());
    }

    /**
     * Returns a codec for a map stored as a JSON CLOB and read raw, for a parameterisation no
     * concrete key and value class can be resolved from.
     *
     * @param gson the Gson instance for serialization
     * @param <K> the key type
     * @param <V> the value type
     * @return a codec reading into a raw map
     */
    @SuppressWarnings("unchecked")
    static <K, V> @NotNull Codec<Map<K, V>> map(@NotNull Gson gson) {
        return (Codec<Map<K, V>>) (Codec<?>) new MapCodec(gson, ConcurrentMap.class);
    }

    /**
     * Returns a codec for an optional stored in a nullable column.
     *
     * <p>An inner type in {@link #NATIVE_SQL_TYPES} binds as that SQL type and crosses the boundary
     * as itself; every other inner type is JSON in a CLOB. Database NULL is
     * {@link Optional#empty()} either way, and an empty string in a {@code String} column is too.
     *
     * @param gson the Gson instance for serialization of a non-native inner type
     * @param innerType the concrete inner class
     * @param <I> the inner type
     * @return a codec for an optional of that inner type
     */
    static <I> @NotNull Codec<Optional<I>> optional(@NotNull Gson gson, @NotNull Class<I> innerType) {
        return new OptionalCodec<>(gson, innerType);
    }

    /**
     * How one column value crosses the JDBC boundary.
     *
     * @param <T> the value one column holds
     */
    public interface Codec<T> {

        /**
         * Returns the SQL type the column binds as.
         *
         * @return a {@link Types} constant
         */
        int sqlType();

        /**
         * Returns the class the property holds.
         *
         * @return the returned class
         */
        @NotNull Class<T> returnedClass();

        /**
         * Reads one column of a row.
         *
         * @param rs the result set positioned on the row
         * @param position the one-based column position
         * @return the value the column holds
         * @throws SQLException if the column cannot be read
         */
        @Nullable T read(@NotNull ResultSet rs, int position) throws SQLException;

        /**
         * Writes one parameter of a statement.
         *
         * @param st the statement being prepared
         * @param value the value to write
         * @param index the one-based parameter index
         * @throws SQLException if the parameter cannot be set
         */
        void write(@NotNull PreparedStatement st, @Nullable T value, int index) throws SQLException;

        /**
         * Returns the cacheable form of a value.
         *
         * @param value the value to disassemble
         * @return the form the second-level cache stores
         */
        @Nullable Serializable disassemble(@Nullable T value);

        /**
         * Rebuilds a value from its cached form.
         *
         * @param cached the form the second-level cache stored
         * @return the rebuilt value
         */
        @Nullable T assemble(@Nullable Serializable cached);

        /**
         * Returns a copy of a value that shares no mutable state with it.
         *
         * @param value the value to copy
         * @return the copy
         */
        @Nullable T copy(@Nullable T value);

    }

    /**
     * Stores a class as JSON in a CLOB, and a null as SQL NULL.
     *
     * @param gson the Gson instance for serialization
     * @param type the stored class
     * @param <T> the stored type
     */
    private record ScalarCodec<T>(@NotNull Gson gson, @NotNull Class<T> type) implements Codec<T> {

        /** {@inheritDoc} */
        @Override
        public int sqlType() {
            return Types.CLOB;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Class<T> returnedClass() {
            return this.type;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable T read(@NotNull ResultSet rs, int position) throws SQLException {
            String json = rs.getString(position);
            return rs.wasNull() ? null : this.gson.fromJson(json, this.type);
        }

        /** {@inheritDoc} */
        @Override
        public void write(@NotNull PreparedStatement st, @Nullable T value, int index) throws SQLException {
            if (value == null)
                st.setNull(index, Types.VARCHAR);
            else
                st.setString(index, this.gson.toJson(value));
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Serializable disassemble(@Nullable T value) {
            return value == null ? null : this.gson.toJson(value);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable T assemble(@Nullable Serializable cached) {
            return cached == null ? null : this.gson.fromJson((String) cached, this.type);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable T copy(@Nullable T value) {
            return value == null ? null : this.gson.fromJson(this.gson.toJson(value), this.type);
        }

    }

    /**
     * Stores a list as a JSON array in a CLOB, and an absent list as an empty array rather than as
     * SQL NULL, so a read never has to answer null.
     *
     * @param gson the Gson instance for serialization
     * @param arrayType the array class the read targets
     * @param <E> the element type
     */
    private record ListCodec<E>(@NotNull Gson gson, @NotNull Class<E[]> arrayType) implements Codec<List<E>> {

        /** {@inheritDoc} */
        @Override
        public int sqlType() {
            return Types.CLOB;
        }

        /** {@inheritDoc} */
        @Override
        @SuppressWarnings("unchecked")
        public @NotNull Class<List<E>> returnedClass() {
            return (Class<List<E>>) (Class<?>) List.class;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull List<E> read(@NotNull ResultSet rs, int position) throws SQLException {
            String json = rs.getString(position);

            if (rs.wasNull() || json == null)
                return Concurrent.newList();

            return Concurrent.newList(this.gson.fromJson(json, this.arrayType));
        }

        /** {@inheritDoc} */
        @Override
        public void write(@NotNull PreparedStatement st, @Nullable List<E> value, int index) throws SQLException {
            st.setString(index, value == null ? "[]" : this.gson.toJson(value));
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Serializable disassemble(@Nullable List<E> value) {
            return value == null ? "[]" : this.gson.toJson(value);
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull List<E> assemble(@Nullable Serializable cached) {
            if (cached == null) return Concurrent.newList();
            return Concurrent.newList(this.gson.fromJson((String) cached, this.arrayType));
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull List<E> copy(@Nullable List<E> value) {
            if (value == null) return Concurrent.newList();
            return value.stream().collect(Concurrent.toList());
        }

    }

    /**
     * Stores a map as a JSON object in a CLOB, and an absent map as an empty object rather than as
     * SQL NULL, so a read never has to answer null.
     *
     * @param gson the Gson instance for serialization
     * @param mapType the map type the read targets, parameterized or raw
     */
    private record MapCodec(@NotNull Gson gson, @NotNull Type mapType) implements Codec<Map<Object, Object>> {

        /** {@inheritDoc} */
        @Override
        public int sqlType() {
            return Types.CLOB;
        }

        /** {@inheritDoc} */
        @Override
        @SuppressWarnings("unchecked")
        public @NotNull Class<Map<Object, Object>> returnedClass() {
            return (Class<Map<Object, Object>>) (Class<?>) Map.class;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Map<Object, Object> read(@NotNull ResultSet rs, int position) throws SQLException {
            String json = rs.getString(position);

            if (rs.wasNull() || json == null)
                return Concurrent.newMap();

            return this.parse(json);
        }

        /** {@inheritDoc} */
        @Override
        public void write(@NotNull PreparedStatement st, @Nullable Map<Object, Object> value, int index) throws SQLException {
            st.setString(index, value == null ? "{}" : this.gson.toJson(value));
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Serializable disassemble(@Nullable Map<Object, Object> value) {
            return value == null ? "{}" : this.gson.toJson(value);
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Map<Object, Object> assemble(@Nullable Serializable cached) {
            return cached == null ? Concurrent.newMap() : this.parse((String) cached);
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Map<Object, Object> copy(@Nullable Map<Object, Object> value) {
            return value == null ? Concurrent.newMap() : this.parse(this.gson.toJson(value));
        }

        private @NotNull Map<Object, Object> parse(@NotNull String json) {
            ConcurrentMap<Object, Object> map = this.gson.fromJson(json, this.mapType);
            return map != null ? map : Concurrent.newMap();
        }

    }

    /**
     * Stores an optional in a nullable column, natively for the wrapper types and as JSON for
     * everything else.
     *
     * @param gson the Gson instance for serialization of a non-native inner type
     * @param innerType the concrete inner class
     * @param <I> the inner type
     */
    private record OptionalCodec<I>(@NotNull Gson gson, @NotNull Class<I> innerType) implements Codec<Optional<I>> {

        /** {@inheritDoc} */
        @Override
        public int sqlType() {
            return NATIVE_SQL_TYPES.getOrDefault(this.innerType, Types.CLOB);
        }

        /** {@inheritDoc} */
        @Override
        @SuppressWarnings("unchecked")
        public @NotNull Class<Optional<I>> returnedClass() {
            return (Class<Optional<I>>) (Class<?>) Optional.class;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Optional<I> read(@NotNull ResultSet rs, int position) throws SQLException {
            if (this.isNative()) {
                I value = rs.getObject(position, this.innerType);

                if (this.innerType == String.class && "".equals(value))
                    return Optional.empty();

                return Optional.ofNullable(value);
            }

            String json = rs.getString(position);

            if (rs.wasNull() || json == null)
                return Optional.empty();

            return Optional.ofNullable(this.gson.fromJson(json, this.innerType));
        }

        /** {@inheritDoc} */
        @Override
        public void write(@NotNull PreparedStatement st, @Nullable Optional<I> value, int index) throws SQLException {
            Optional<?> optional = value instanceof Optional<?> present ? present : Optional.empty();

            if (optional.isPresent() && this.innerType == String.class && "".equals(optional.get()))
                optional = Optional.empty();

            if (optional.isEmpty()) {
                st.setNull(index, this.sqlType());
            } else if (this.isNative()) {
                st.setObject(index, optional.get(), this.sqlType());
            } else {
                st.setString(index, this.gson.toJson(optional.get()));
            }
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable Serializable disassemble(@Nullable Optional<I> value) {
            if (value == null || value.isEmpty()) return null;
            if (this.isNative()) return (Serializable) value.get();
            return this.gson.toJson(value.get());
        }

        /** {@inheritDoc} */
        @Override
        @SuppressWarnings("unchecked")
        public @NotNull Optional<I> assemble(@Nullable Serializable cached) {
            if (cached == null) return Optional.empty();
            if (this.isNative()) return Optional.of((I) cached);
            return Optional.ofNullable(this.gson.fromJson((String) cached, this.innerType));
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull Optional<I> copy(@Nullable Optional<I> value) {
            if (value == null || value.isEmpty()) return Optional.empty();
            if (this.isNative()) return value;
            return Optional.ofNullable(this.gson.fromJson(this.gson.toJson(value.get()), this.innerType));
        }

        private boolean isNative() {
            return NATIVE_SQL_TYPES.containsKey(this.innerType);
        }

    }

    /**
     * Discovers the four field shapes across the entity models and registers a
     * {@link GsonValueType} for each parameterisation found.
     *
     * <p>Hibernate resolves property types eagerly while the metadata is built, matching a field to
     * a registered type by its raw class name, so a shape with more than one parameterisation
     * cannot be bound by name alone. Each container shape therefore also registers one raw-typed
     * instance under the container's own name, and then re-points every property at its own
     * instance in {@link #postProcess} through {@link BasicValue.Resolution#updateResolution}.
     */
    public static final class Registrar implements TypeRegistrar {

        private final @NotNull List<Shape> shapes = List.of(
            new Annotated(),
            Container.list(),
            Container.optional(),
            new Mapping()
        );

        private Gson gson;

        /** {@inheritDoc} */
        @Override
        public void scan(@NotNull Gson gson, @NotNull Iterable<Class<JpaModel>> models) {
            this.gson = gson;

            for (Class<JpaModel> modelClass : models) {
                Reflection<JpaModel> reflection = new Reflection<>(modelClass);
                reflection.setProcessingSuperclass(false);

                for (FieldAccessor<?> accessor : reflection.getFields()) {
                    if (!TypeRegistrar.isPersistentField(accessor)) continue;

                    for (Shape shape : this.shapes)
                        shape.match(accessor);
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public void register(@NotNull MetadataBuilder builder) {
            this.shapes.forEach(shape -> shape.register(this.gson, builder));
        }

        /** {@inheritDoc} */
        @Override
        public void postProcess(@NotNull Metadata metadata) {
            this.shapes.forEach(shape -> shape.postProcess(metadata));
        }

    }

    /**
     * One field shape - what it matches, what it registers, and how it re-points the properties it
     * matched.
     */
    private interface Shape {

        /**
         * Records what one entity field contributes, or nothing when the field is a different shape.
         *
         * @param accessor the field to inspect
         */
        void match(@NotNull FieldAccessor<?> accessor);

        /**
         * Registers a type per parameterisation recorded, plus any fallback the shape needs.
         *
         * @param gson the session's Gson instance
         * @param builder the metadata builder to register with
         */
        void register(@NotNull Gson gson, @NotNull MetadataBuilder builder);

        /**
         * Re-points the matched properties from their eagerly-resolved binding to the instance
         * registered for their own parameterisation.
         *
         * @param metadata the built metadata whose property bindings may be upgraded
         */
        default void postProcess(@NotNull Metadata metadata) {}

    }

    /**
     * A class carrying {@link GsonType @GsonType} held directly by a field. It has one
     * parameterisation per class and its own name to register under, so it needs neither a
     * fallback nor a re-point.
     */
    private static final class Annotated implements Shape {

        private final Set<Class<?>> types = new LinkedHashSet<>();

        /** {@inheritDoc} */
        @Override
        public void match(@NotNull FieldAccessor<?> accessor) {
            Class<?> fieldType = accessor.getFieldType();

            if (fieldType.isAnnotationPresent(GsonType.class))
                this.types.add(fieldType);
        }

        /** {@inheritDoc} */
        @Override
        public void register(@NotNull Gson gson, @NotNull MetadataBuilder builder) {
            this.types.forEach(type -> builder.applyBasicType(new GsonValueType<>(scalar(gson, type)), type.getName()));
        }

    }

    /**
     * A container taking one type argument - a {@code List<E>} or an {@code Optional<I>}. The two
     * differ in what they accept as an argument and in what they fall back to, and in nothing else.
     */
    private static final class Container implements Shape {

        private final @NotNull String prefix;
        private final @NotNull Class<?> rawFieldType;
        private final @NotNull Function<Type, Class<?>> argument;
        private final @NotNull Class<?> fallbackArgument;
        private final boolean fallbackUnderEveryRawName;
        private final @NotNull BiFunction<Gson, Class<?>, Codec<?>> codecs;

        private final Map<Class<?>, String> typeKeys = new LinkedHashMap<>();
        private final Set<String> rawClassNames = new LinkedHashSet<>();

        private Container(
            @NotNull String prefix,
            @NotNull Class<?> rawFieldType,
            @NotNull Function<Type, Class<?>> argument,
            @NotNull Class<?> fallbackArgument,
            boolean fallbackUnderEveryRawName,
            @NotNull BiFunction<Gson, Class<?>, Codec<?>> codecs
        ) {
            this.prefix = prefix;
            this.rawFieldType = rawFieldType;
            this.argument = argument;
            this.fallbackArgument = fallbackArgument;
            this.fallbackUnderEveryRawName = fallbackUnderEveryRawName;
            this.codecs = codecs;
        }

        /**
         * Returns the list shape, which falls back to an {@code Object} element under every raw list
         * class it saw, because a field may be declared as any of them.
         *
         * @return the list shape
         */
        private static @NotNull Container list() {
            return new Container("GsonList", List.class, Container::element, Object.class, true, GsonValueType::list);
        }

        /**
         * Returns the optional shape, which falls back to a {@code String} inner type under
         * {@link Optional}'s own name, because that is the only class a field can be declared as.
         *
         * @return the optional shape
         */
        private static @NotNull Container optional() {
            return new Container("GsonOptional", Optional.class, Container::inner, String.class, false, GsonValueType::optional);
        }

        /**
         * Returns the element a list registers an instance for, or {@code null} when it has none.
         *
         * <p>A parameterized element contributes its raw type, so a list of maps registers for the
         * map. An element that cannot be instantiated has no instance to register, and one that is
         * an entity in its own right belongs to the association machinery rather than to a column.
         *
         * @param argument the declared element type
         * @return the element class, or {@code null} to leave the field on the fallback
         */
        private static @Nullable Class<?> element(@NotNull Type argument) {
            Class<?> raw = argument instanceof Class<?> plain ? plain
                : argument instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> rawType ? rawType
                : null;

            if (raw == null || raw.isInterface() || Modifier.isAbstract(raw.getModifiers())) return null;

            return JpaModel.class.isAssignableFrom(raw) ? null : raw;
        }

        /**
         * Returns the inner type an optional registers an instance for, or {@code null} when it has
         * none.
         *
         * <p>Any plain class is taken, including an interface and including an entity - an optional
         * column is a value either way, and nothing resolves one to an association. A parameterized
         * inner type is refused, because its arguments would be erased by the read.
         *
         * @param argument the declared inner type
         * @return the inner class, or {@code null} to leave the field on the fallback
         */
        private static @Nullable Class<?> inner(@NotNull Type argument) {
            return argument instanceof Class<?> plain ? plain : null;
        }

        /** {@inheritDoc} */
        @Override
        public void match(@NotNull FieldAccessor<?> accessor) {
            if (!(accessor.getGenericType() instanceof ParameterizedType parameterized)) return;
            if (!(parameterized.getRawType() instanceof Class<?> rawType) || !this.rawFieldType.isAssignableFrom(rawType)) return;
            if (parameterized.getActualTypeArguments().length != 1) return;

            if (this.fallbackUnderEveryRawName)
                this.rawClassNames.add(rawType.getName());

            Class<?> argument = this.argument.apply(parameterized.getActualTypeArguments()[0]);

            if (argument != null)
                this.typeKeys.putIfAbsent(argument, this.prefix + ":" + argument.getName());
        }

        /** {@inheritDoc} */
        @Override
        public void register(@NotNull Gson gson, @NotNull MetadataBuilder builder) {
            this.typeKeys.forEach((argument, key) ->
                builder.applyBasicType(new GsonValueType<>(this.codecs.apply(gson, argument)), key));

            if (this.fallbackUnderEveryRawName) {
                if (!this.rawClassNames.isEmpty())
                    builder.applyBasicType(
                        new GsonValueType<>(this.codecs.apply(gson, this.fallbackArgument)),
                        this.rawClassNames.toArray(String[]::new)
                    );
            } else if (!this.typeKeys.isEmpty())
                builder.applyBasicType(
                    new GsonValueType<>(this.codecs.apply(gson, this.fallbackArgument)),
                    this.rawFieldType.getName()
                );
        }

        /** {@inheritDoc} */
        @Override
        public void postProcess(@NotNull Metadata metadata) {
            if (!this.typeKeys.isEmpty())
                TypeRegistrar.bindTypes(metadata, this.typeKeys, this.rawFieldType);
        }

    }

    /**
     * A {@code Map<K, V>}, which registers under a key naming both of its arguments and falls back
     * to a raw read under every raw map class it saw.
     */
    private static final class Mapping implements Shape {

        private final Map<String, Class<?>[]> typeKeys = new LinkedHashMap<>();
        private final Set<String> rawClassNames = new LinkedHashSet<>();

        /** {@inheritDoc} */
        @Override
        public void match(@NotNull FieldAccessor<?> accessor) {
            if (!(accessor.getGenericType() instanceof ParameterizedType parameterized)) {
                if (Map.class.isAssignableFrom(accessor.getFieldType()))
                    this.rawClassNames.add(accessor.getFieldType().getName());

                return;
            }

            if (!(parameterized.getRawType() instanceof Class<?> rawType) || !Map.class.isAssignableFrom(rawType)) return;
            if (parameterized.getActualTypeArguments().length != 2) return;

            this.rawClassNames.add(rawType.getName());

            Class<?> keyClass = concreteClassOf(parameterized.getActualTypeArguments()[0]);
            Class<?> valueClass = concreteClassOf(parameterized.getActualTypeArguments()[1]);

            if (keyClass == null || valueClass == null) return;
            if (JpaModel.class.isAssignableFrom(keyClass)) return;
            if (JpaModel.class.isAssignableFrom(valueClass)) return;

            this.typeKeys.putIfAbsent("GsonMap:" + keyClass.getName() + ":" + valueClass.getName(), new Class<?>[]{ keyClass, valueClass });
        }

        /** {@inheritDoc} */
        @Override
        public void register(@NotNull Gson gson, @NotNull MetadataBuilder builder) {
            this.typeKeys.forEach((key, types) ->
                builder.applyBasicType(new GsonValueType<>(map(gson, types[0], types[1])), key));

            if (!this.rawClassNames.isEmpty())
                builder.applyBasicType(
                    new GsonValueType<>(map(gson)),
                    this.rawClassNames.toArray(String[]::new)
                );
        }

        /** {@inheritDoc} */
        @Override
        public void postProcess(@NotNull Metadata metadata) {
            if (!this.typeKeys.isEmpty())
                TypeRegistrar.bindMapTypes(metadata, this.typeKeys);
        }

        private static @Nullable Class<?> concreteClassOf(@NotNull Type type) {
            if (type instanceof Class<?> plain)
                return plain.isInterface() || Modifier.isAbstract(plain.getModifiers()) ? null : plain;

            if (type instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> raw)
                return raw.isInterface() || Modifier.isAbstract(raw.getModifiers()) ? null : raw;

            return null;
        }

    }

}

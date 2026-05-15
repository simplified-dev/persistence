package dev.simplified.persistence.type;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.mapping.BasicValue.Resolution;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Hibernate {@link UserType} that serializes {@code Optional<T>} to and from a nullable column.
 * <p>
 * Primitive wrappers and {@link String} use their native SQL types via direct JDBC and bypass
 * Gson entirely; all other inner types are stored as JSON CLOB via Gson. Database NULL maps to
 * {@link Optional#empty()}, and empty strings are normalized to absent for {@code Optional<String>}.
 * <p>
 * Registration is fully automatic via the inner {@link Registrar}:
 * <ol>
 *     <li>A per-inner-type instance is registered under an inner-type-specific key
 *         (e.g. {@code "GsonOptional:java.lang.Integer"}).</li>
 *     <li>A default {@code String}-inner fallback is registered under
 *         {@code java.util.Optional} so Hibernate's eager type resolution always finds
 *         a valid binding.</li>
 *     <li>After metadata build, {@link Registrar#postProcess} upgrades each property's
 *         binding to the correct inner-typed instance via
 *         {@link Resolution#updateResolution}.</li>
 * </ol>
 *
 * @param <T> the inner type wrapped by {@link Optional}
 */
@SuppressWarnings("unchecked")
public final class GsonOptionalType<T> implements UserType<Optional<T>> {

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

    private final @NotNull Gson gson;
    private final @NotNull Class<T> innerType;
    private final int sqlType;
    private final boolean nativeType;

    /**
     * Constructs a new optional type for the given inner class.
     *
     * @param gson the Gson instance for serialization of non-native inner types
     * @param innerType the concrete inner class (e.g. {@code String.class}, {@code Color.class})
     */
    public GsonOptionalType(@NotNull Gson gson, @NotNull Class<T> innerType) {
        this.gson = gson;
        this.innerType = innerType;
        this.nativeType = NATIVE_SQL_TYPES.containsKey(innerType);
        this.sqlType = NATIVE_SQL_TYPES.getOrDefault(innerType, Types.CLOB);
    }

    /** {@inheritDoc} */
    @Override
    public int getSqlType() {
        return this.sqlType;
    }

    /** {@inheritDoc} */
    @Override
    public Class<Optional<T>> returnedClass() {
        return (Class<Optional<T>>) (Class<?>) Optional.class;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Optional<T> x, Optional<T> y) {
        return Objects.equals(x, y);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode(Optional<T> x) {
        return Objects.hashCode(x);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<T> nullSafeGet(@NotNull ResultSet rs, int position, WrapperOptions options) throws SQLException {
        if (this.nativeType) {
            T value = rs.getObject(position, this.innerType);

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
    public void nullSafeSet(@NotNull PreparedStatement st, Optional<T> value, int index, WrapperOptions options) throws SQLException {
        Optional<?> optional = value instanceof Optional<?> opt ? opt : Optional.empty();

        if (optional.isPresent() && this.innerType == String.class && "".equals(optional.get()))
            optional = Optional.empty();

        if (optional.isEmpty()) {
            st.setNull(index, this.sqlType);
        } else if (this.nativeType) {
            st.setObject(index, optional.get(), this.sqlType);
        } else {
            st.setString(index, this.gson.toJson(optional.get()));
        }
    }

    /** {@inheritDoc} */
    @Override
    public Optional<T> deepCopy(Optional<T> value) {
        if (value.isEmpty()) return Optional.empty();
        if (this.nativeType) return value;
        return Optional.ofNullable(this.gson.fromJson(this.gson.toJson(value.get()), this.innerType));
    }

    /** {@inheritDoc} */
    @Override
    public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public Serializable disassemble(Optional<T> value) {
        if (value == null || value.isEmpty()) return null;
        if (this.nativeType) return (Serializable) value.get();
        return this.gson.toJson(value.get());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Optional<T> assemble(Serializable cached, Object owner) {
        if (cached == null) return Optional.empty();
        if (this.nativeType) return Optional.of((T) cached);
        return Optional.ofNullable(this.gson.fromJson((String) cached, this.innerType));
    }

    /** {@inheritDoc} */
    @Override
    public Optional<T> replace(Optional<T> original, Optional<T> target, Object owner) {
        return this.deepCopy(original);
    }

    /**
     * Discovers {@code Optional<T>} entity fields with concrete inner types and registers
     * a {@link GsonOptionalType} per unique inner class, plus a default {@code String}-inner
     * fallback under {@code java.util.Optional} for Hibernate 6+ eager resolution.
     */
    public static final class Registrar implements TypeRegistrar {

        private final Map<Class<?>, String> typeKeys = new LinkedHashMap<>();
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
                    if (accessor.getFieldType() != Optional.class) continue;

                    Type genericType = accessor.getGenericType();
                    if (!(genericType instanceof ParameterizedType pt)) continue;
                    if (pt.getActualTypeArguments().length != 1) continue;
                    if (!(pt.getActualTypeArguments()[0] instanceof Class<?> innerClass)) continue;

                    this.typeKeys.putIfAbsent(innerClass, "GsonOptional:" + innerClass.getName());
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public void register(@NotNull MetadataBuilder builder) {
            this.typeKeys.forEach((innerClass, key) ->
                builder.applyBasicType(new GsonOptionalType(this.gson, innerClass), key));

            if (!this.typeKeys.isEmpty())
                builder.applyBasicType(
                    new GsonOptionalType(this.gson, String.class),
                    Optional.class.getName()
                );
        }

        /** {@inheritDoc} */
        @Override
        public void postProcess(@NotNull Metadata metadata) {
            if (!this.typeKeys.isEmpty())
                TypeRegistrar.bindTypes(metadata, this.typeKeys, Optional.class);
        }

    }

}

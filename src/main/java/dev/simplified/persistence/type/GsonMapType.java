package dev.simplified.persistence.type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.simplified.persistence.JpaModel;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Hibernate {@link UserType} that serializes any {@code Map<K, V>} to and from a JSON CLOB
 * column using Gson.
 *
 * <p>Registration is automatic via the inner {@link Registrar}:
 * <ol>
 *     <li>A per-key/value-type instance is registered under a type-specific key
 *         (e.g. {@code "GsonMap:java.lang.Integer:ItemTintInfo"}).</li>
 *     <li>A default {@code Object}-typed fallback is registered under every raw
 *         {@code Map} subclass FQCN seen in entity fields so Hibernate's eager type
 *         resolution always finds a valid binding.</li>
 *     <li>After metadata build, {@link Registrar#postProcess} upgrades each property's
 *         binding to the correct typed instance via
 *         {@link org.hibernate.mapping.BasicValue.Resolution#updateResolution}.</li>
 * </ol>
 */
@SuppressWarnings("unchecked")
public final class GsonMapType implements UserType<Map<?, ?>> {

    private final @NotNull Gson gson;
    private final @NotNull Type mapType;

    /**
     * Constructs a typed map type that deserializes with full generic type information.
     *
     * @param gson the Gson instance for serialization
     * @param keyClass the map key class
     * @param valueClass the map value class
     */
    public GsonMapType(@NotNull Gson gson, @NotNull Class<?> keyClass, @NotNull Class<?> valueClass) {
        this.gson = gson;
        this.mapType = TypeToken.getParameterized(ConcurrentMap.class, keyClass, valueClass).getType();
    }

    /**
     * Constructs a fallback map type using raw {@link ConcurrentMap} deserialization.
     *
     * @param gson the Gson instance for serialization
     */
    public GsonMapType(@NotNull Gson gson) {
        this.gson = gson;
        this.mapType = ConcurrentMap.class;
    }

    /** {@inheritDoc} */
    @Override
    public int getSqlType() {
        return Types.CLOB;
    }

    /** {@inheritDoc} */
    @Override
    public Class<Map<?, ?>> returnedClass() {
        return (Class<Map<?, ?>>) (Class<?>) Map.class;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Map<?, ?> x, Map<?, ?> y) {
        return Objects.equals(x, y);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode(Map<?, ?> x) {
        return Objects.hashCode(x);
    }

    /** {@inheritDoc} */
    @Override
    public Map<?, ?> nullSafeGet(@NotNull ResultSet rs, int position, WrapperOptions options) throws SQLException {
        String json = rs.getString(position);

        if (rs.wasNull() || json == null)
            return Concurrent.newMap();

        ConcurrentMap<?, ?> map = this.gson.fromJson(json, this.mapType);
        return map != null ? map : Concurrent.newMap();
    }

    /** {@inheritDoc} */
    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, Map<?, ?> value, int index, WrapperOptions options) throws SQLException {
        st.setString(index, value == null ? "{}" : this.gson.toJson(value));
    }

    /** {@inheritDoc} */
    @Override
    public Map<?, ?> deepCopy(Map<?, ?> value) {
        if (value == null) return Concurrent.newMap();
        ConcurrentMap<?, ?> copy = this.gson.fromJson(this.gson.toJson(value), this.mapType);
        return copy != null ? copy : Concurrent.newMap();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public Serializable disassemble(Map<?, ?> value) {
        return value == null ? "{}" : this.gson.toJson(value);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Map<?, ?> assemble(Serializable cached, Object owner) {
        if (cached == null) return Concurrent.newMap();
        ConcurrentMap<?, ?> map = this.gson.fromJson((String) cached, this.mapType);
        return map != null ? map : Concurrent.newMap();
    }

    /** {@inheritDoc} */
    @Override
    public Map<?, ?> replace(Map<?, ?> original, Map<?, ?> target, Object owner) {
        return this.deepCopy(original);
    }

    /**
     * Discovers entity fields assignable to {@link Map} and registers a per-key/value-type
     * {@link GsonMapType} for each unique parameterization, plus a default fallback
     * under each raw {@code Map} subclass FQCN for Hibernate 6+ eager resolution.
     */
    public static final class Registrar implements TypeRegistrar {

        private final Map<String, Class<?>[]> typeKeys = new LinkedHashMap<>();
        private final Set<String> rawClassNames = new LinkedHashSet<>();
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

                    Type genericType = accessor.getGenericType();
                    if (!(genericType instanceof ParameterizedType pt)) {
                        if (Map.class.isAssignableFrom(accessor.getFieldType()))
                            this.rawClassNames.add(accessor.getFieldType().getName());
                        continue;
                    }
                    if (!(pt.getRawType() instanceof Class<?> rawType) || !Map.class.isAssignableFrom(rawType))
                        continue;
                    if (pt.getActualTypeArguments().length != 2)
                        continue;

                    this.rawClassNames.add(rawType.getName());

                    Class<?> keyClass = extractClass(pt.getActualTypeArguments()[0]);
                    Class<?> valClass = extractClass(pt.getActualTypeArguments()[1]);

                    if (keyClass == null || valClass == null) continue;
                    if (JpaModel.class.isAssignableFrom(keyClass)) continue;
                    if (JpaModel.class.isAssignableFrom(valClass)) continue;

                    String key = "GsonMap:" + keyClass.getName() + ":" + valClass.getName();
                    this.typeKeys.putIfAbsent(key, new Class<?>[]{ keyClass, valClass });
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public void register(@NotNull MetadataBuilder builder) {
            this.typeKeys.forEach((key, types) ->
                builder.applyBasicType(new GsonMapType(this.gson, types[0], types[1]), key));

            if (!this.rawClassNames.isEmpty())
                builder.applyBasicType(
                    new GsonMapType(this.gson),
                    this.rawClassNames.toArray(String[]::new)
                );
        }

        /** {@inheritDoc} */
        @Override
        public void postProcess(@NotNull Metadata metadata) {
            if (!this.typeKeys.isEmpty())
                TypeRegistrar.bindMapTypes(metadata, this.typeKeys);
        }

        private static Class<?> extractClass(Type type) {
            if (type instanceof Class<?> c) {
                if (c.isInterface() || Modifier.isAbstract(c.getModifiers())) return null;
                return c;
            }
            if (type instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> raw) {
                if (raw.isInterface() || Modifier.isAbstract(raw.getModifiers())) return null;
                return raw;
            }
            return null;
        }

    }

}

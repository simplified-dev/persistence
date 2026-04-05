package dev.simplified.persistence.type;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.collection.concurrent.Concurrent;
import dev.simplified.collection.concurrent.ConcurrentList;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.mapping.BasicValue;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;

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
import java.util.Set;

/**
 * Hibernate {@link UserType} that serializes a {@code List<T>} to and from a JSON CLOB column
 * using Gson.
 * <p>
 * Deserialization targets a {@code T[]} array type so Gson preserves the concrete element class
 * (e.g. {@code Integer[].class} avoids Gson's default-to-double behaviour for JSON numbers).
 * The resulting array is wrapped in a {@link ConcurrentList}.
 * <p>
 * Registration is fully automatic via the inner {@link Registrar}:
 * <ol>
 *     <li>A per-element-type instance is registered under an element-specific key
 *         (e.g. {@code "GsonList:java.lang.String"}).</li>
 *     <li>A default {@code Object}-element fallback is registered under every raw
 *         {@code List} subclass FQCN seen in entity fields so Hibernate's eager type
 *         resolution always finds a valid binding.</li>
 *     <li>After metadata build, {@link Registrar#postProcess} upgrades each property's
 *         binding to the correct element-typed instance via
 *         {@link BasicValue.Resolution#updateResolution}.</li>
 * </ol>
 *
 * @param <T> the list element type
 */
@SuppressWarnings("unchecked")
public final class GsonListType<T> implements UserType<List<T>> {

    private final @NotNull Gson gson;
    private final @NotNull Class<T[]> arrayType;

    /**
     * Constructs a new list type for the given element class.
     *
     * @param gson the Gson instance for serialization
     * @param elementType the concrete element class
     */
    public GsonListType(@NotNull Gson gson, @NotNull Class<T> elementType) {
        this.gson = gson;
        this.arrayType = (Class<T[]>) Array.newInstance(elementType, 0).getClass();
    }

    /** {@inheritDoc} */
    @Override
    public int getSqlType() {
        return Types.CLOB;
    }

    /** {@inheritDoc} */
    @Override
    public Class<List<T>> returnedClass() {
        return (Class<List<T>>) (Class<?>) List.class;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(List<T> x, List<T> y) {
        return Objects.equals(x, y);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode(List<T> x) {
        return Objects.hashCode(x);
    }

    /** {@inheritDoc} */
    @Override
    public List<T> nullSafeGet(@NotNull ResultSet rs, int position, WrapperOptions options) throws SQLException {
        String json = rs.getString(position);

        if (rs.wasNull() || json == null)
            return Concurrent.newList();

        return Concurrent.newList(this.gson.fromJson(json, this.arrayType));
    }

    /** {@inheritDoc} */
    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, List<T> value, int index, WrapperOptions options) throws SQLException {
        st.setString(index, value == null ? "[]" : this.gson.toJson(value));
    }

    /** {@inheritDoc} */
    @Override
    public List<T> deepCopy(List<T> value) {
        if (value == null) return Concurrent.newList();
        return value.stream().collect(Concurrent.toList());
    }

    /** {@inheritDoc} */
    @Override
    public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public Serializable disassemble(List<T> value) {
        return value == null ? "[]" : this.gson.toJson(value);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull List<T> assemble(Serializable cached, Object owner) {
        if (cached == null) return Concurrent.newList();
        T[] arr = this.gson.fromJson((String) cached, this.arrayType);
        return Concurrent.newList(arr);
    }

    /** {@inheritDoc} */
    @Override
    public List<T> replace(List<T> original, List<T> target, Object owner) {
        return this.deepCopy(original);
    }

    /**
     * Discovers {@code List<T>} entity fields with concrete, non-entity element types and
     * registers a {@link GsonListType} per unique element class, plus a default fallback
     * under each raw {@code List} subclass FQCN for Hibernate 6+ eager resolution.
     */
    public static final class Registrar implements TypeRegistrar {

        private final Map<Class<?>, String> typeKeys = new LinkedHashMap<>();
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
                    if (!(genericType instanceof ParameterizedType pt)) continue;
                    if (!(pt.getRawType() instanceof Class<?> rawType) || !List.class.isAssignableFrom(rawType)) continue;
                    if (pt.getActualTypeArguments().length != 1) continue;

                    this.rawClassNames.add(rawType.getName());

                    Type elemType = pt.getActualTypeArguments()[0];
                    Class<?> elemClass;
                    if (elemType instanceof Class<?> c)
                        elemClass = c;
                    else if (elemType instanceof ParameterizedType ept && ept.getRawType() instanceof Class<?> rawElem)
                        elemClass = rawElem;
                    else
                        continue;

                    if (elemClass.isInterface() || Modifier.isAbstract(elemClass.getModifiers())) continue;
                    if (JpaModel.class.isAssignableFrom(elemClass)) continue;

                    this.typeKeys.putIfAbsent(elemClass, "GsonList:" + elemClass.getName());
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public void register(@NotNull MetadataBuilder builder) {
            this.typeKeys.forEach((elemClass, key) ->
                builder.applyBasicType(new GsonListType(this.gson, elemClass), key));

            if (!this.rawClassNames.isEmpty())
                builder.applyBasicType(
                    new GsonListType(this.gson, Object.class),
                    this.rawClassNames.toArray(String[]::new)
                );
        }

        /** {@inheritDoc} */
        @Override
        public void postProcess(@NotNull Metadata metadata) {
            if (!this.typeKeys.isEmpty())
                TypeRegistrar.bindTypes(metadata, this.typeKeys, List.class);
        }

    }

}

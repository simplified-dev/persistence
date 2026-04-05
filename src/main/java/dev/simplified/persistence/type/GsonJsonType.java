package dev.simplified.persistence.type;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import lombok.RequiredArgsConstructor;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Hibernate {@link UserType} that serializes a {@link GsonType @GsonType}-annotated class
 * to and from a JSON CLOB column using Gson.
 * <p>
 * Each annotated class is registered under its FQCN via
 * {@link MetadataBuilder#applyBasicType(UserType, String...)}, so Hibernate auto-resolves
 * entity fields whose declared type matches - no per-field {@code @Type} annotation needed.
 *
 * @param <T> the serialized type
 */
@RequiredArgsConstructor
public final class GsonJsonType<T> implements UserType<T> {

    private final @NotNull Gson gson;
    private final @NotNull Class<T> type;

    /** {@inheritDoc} */
    @Override
    public int getSqlType() {
        return Types.CLOB;
    }

    /** {@inheritDoc} */
    @Override
    public Class<T> returnedClass() {
        return this.type;
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
    public @Nullable T nullSafeGet(@NotNull ResultSet rs, int position, WrapperOptions options) throws SQLException {
        String json = rs.getString(position);
        return rs.wasNull() ? null : this.gson.fromJson(json, this.type);
    }

    /** {@inheritDoc} */
    @Override
    public void nullSafeSet(@NotNull PreparedStatement st, @Nullable T value, int index, WrapperOptions options) throws SQLException {
        if (value == null)
            st.setNull(index, Types.VARCHAR);
        else
            st.setString(index, this.gson.toJson(value));
    }

    /** {@inheritDoc} */
    @Override
    public T deepCopy(T value) {
        if (value == null) return null;
        return this.gson.fromJson(this.gson.toJson(value), this.type);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public Serializable disassemble(T value) {
        return value == null ? null : this.gson.toJson(value);
    }

    /** {@inheritDoc} */
    @Override
    public T assemble(Serializable cached, Object owner) {
        return cached == null ? null : this.gson.fromJson((String) cached, this.type);
    }

    /** {@inheritDoc} */
    @Override
    public T replace(T original, T target, Object owner) {
        return this.deepCopy(original);
    }

    /**
     * Discovers entity fields whose declared type carries {@link GsonType @GsonType}
     * and registers a {@link GsonJsonType} per unique annotated class.
     */
    public static final class Registrar implements TypeRegistrar {

        private final Set<Class<?>> gsonTypes = new LinkedHashSet<>();
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

                    Class<?> fieldType = accessor.getFieldType();
                    if (fieldType.isAnnotationPresent(GsonType.class))
                        this.gsonTypes.add(fieldType);
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public void register(@NotNull MetadataBuilder builder) {
            this.gsonTypes.forEach(type ->
                builder.applyBasicType(new GsonJsonType<>(this.gson, type), type.getName()));
        }

    }

}

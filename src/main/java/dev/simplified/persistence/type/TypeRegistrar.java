package dev.simplified.persistence.type;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import jakarta.persistence.Convert;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.mapping.BasicValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.type.BasicType;
import org.hibernate.type.descriptor.converter.spi.BasicValueConverter;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.java.MutabilityPlan;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Pluggable registration interface for Hibernate custom types.
 * <p>
 * Implementations are discovered reflectively by {@link JpaSession} via classpath
 * scanning of this package - adding a new registrar requires no changes to
 * {@code JpaSession}.
 * <p>
 * Lifecycle:
 * <ol>
 *     <li>{@link #scan} - inspects entity fields to discover types requiring custom handling</li>
 *     <li>{@link #register} - registers discovered types with the {@link MetadataBuilder}</li>
 *     <li>(metadata build)</li>
 *     <li>{@link #postProcess} - upgrades eagerly-resolved default bindings to per-field
 *         type instances by replacing the {@link BasicValue.Resolution} on each property</li>
 * </ol>
 * <p>
 * Hibernate 6+ resolves property types eagerly during {@code metadataBuilder.build()},
 * matching fields to registered types by class FQCN. Types with multiple parameterized
 * instances (e.g. {@link GsonListType}, {@link GsonOptionalType}) register a single
 * default instance under the raw class FQCN so every field gets a valid initial binding,
 * then upgrade individual fields to per-element-type instances in {@link #postProcess}.
 */
public interface TypeRegistrar {

    /** Cached accessor for the {@code BasicValue.resolution} field used by {@link #resolveAndUpdate}. */
    FieldAccessor<BasicValue.Resolution<?>> RESOLUTION_ACCESSOR = new Reflection<>(BasicValue.class).getField("resolution");

    /**
     * Scans entity model fields to discover types that need custom Hibernate type registration.
     *
     * @param gson the session's Gson instance for constructing type handlers
     * @param models the topologically sorted entity classes to inspect
     */
    void scan(@NotNull Gson gson, @NotNull Iterable<Class<JpaModel>> models);

    /**
     * Registers discovered types with the Hibernate {@link MetadataBuilder}.
     *
     * @param builder the metadata builder to register types with
     */
    void register(@NotNull MetadataBuilder builder);

    /**
     * Upgrades eagerly-resolved type bindings after {@code metadataBuilder.build()}.
     * <p>
     * In Hibernate 6+, types are resolved during {@code build()} by matching the field's
     * raw class FQCN against the basic type registry. For parameterized types that require
     * per-field instances (e.g. {@code List<String>} vs {@code List<Level>}), this method
     * replaces the default binding with the correct element-typed instance by injecting a
     * new {@link BasicValue.Resolution} backed by the per-element-type
     * {@link org.hibernate.usertype.UserType UserType}.
     *
     * @param metadata the built metadata whose property bindings may be upgraded
     */
    default void postProcess(@NotNull Metadata metadata) {}

    /**
     * Returns {@code true} if the given field is a persistent Hibernate-mapped column
     * (not static, not {@code transient}, not {@link jakarta.persistence.Transient @Transient}).
     *
     * @param accessor the field to check
     * @return {@code true} if the field should be mapped by Hibernate
     */
    static boolean isPersistentField(@NotNull FieldAccessor<?> accessor) {
        return !accessor.isStatic()
            && !accessor.isTransient()
            && !accessor.hasAnnotation(jakarta.persistence.Transient.class);
    }

    /**
     * Upgrades property bindings for parameterized fields (e.g. {@code List<T>},
     * {@code Optional<T>}) from the default FQCN-resolved type to a per-element-type
     * instance looked up by key in {@code innerTypeToKey}.
     *
     * @param metadata the built Hibernate metadata containing entity bindings
     * @param innerTypeToKey maps each inner type class to its registered type key
     * @param rawFieldType the raw type to match (e.g. {@code List.class}, {@code Optional.class})
     */
    static void bindTypes(@NotNull Metadata metadata, @NotNull Map<Class<?>, String> innerTypeToKey, @NotNull Class<?> rawFieldType) {
        forEachBindableProperty(metadata, (bv, accessor) -> {
            Type genericType = accessor.getGenericType();
            if (!(genericType instanceof ParameterizedType pt)) return;
            if (!(pt.getRawType() instanceof Class<?> rt) || !rawFieldType.isAssignableFrom(rt)) return;
            if (pt.getActualTypeArguments().length != 1) return;

            Type innerType = pt.getActualTypeArguments()[0];
            Class<?> innerClass;
            if (innerType instanceof Class<?> c)
                innerClass = c;
            else if (innerType instanceof ParameterizedType ipt && ipt.getRawType() instanceof Class<?> rawInner)
                innerClass = rawInner;
            else
                return;

            String key = innerTypeToKey.get(innerClass);
            if (key != null) resolveAndUpdate(bv, key);
        });
    }

    /**
     * Upgrades property bindings for all fields whose raw type matches
     * {@code rawFieldType} to a single shared type looked up by {@code key}.
     * <p>
     * Unlike the {@linkplain #bindTypes(Metadata, Map, Class) parameterized overload},
     * this does not inspect generic type arguments - it applies one key to every
     * matching field. Intended for types like {@link Map} where a single
     * {@link org.hibernate.usertype.UserType UserType} instance handles all
     * parameterized variants.
     *
     * @param metadata the built Hibernate metadata containing entity bindings
     * @param key the registered type name to resolve (e.g. {@code "GsonMap"})
     * @param rawFieldType the raw type to match (e.g. {@code Map.class})
     */
    static void bindTypes(@NotNull Metadata metadata, @NotNull String key, @NotNull Class<?> rawFieldType) {
        forEachBindableProperty(metadata, (bv, accessor) -> {
            if (rawFieldType.isAssignableFrom(accessor.getFieldType()))
                resolveAndUpdate(bv, key);
        });
    }

    /**
     * Upgrades property bindings for {@link Map}-typed fields from the default
     * FQCN-resolved type to a per-key/value-type instance.
     *
     * <p>For each map field with two resolved type arguments, constructs the composite key
     * {@code "GsonMap:keyClass:valueClass"} and upgrades the binding if a matching type
     * was registered.
     *
     * @param metadata the built Hibernate metadata containing entity bindings
     * @param typeKeys maps composite key strings to their key/value class pairs
     */
    static void bindMapTypes(@NotNull Metadata metadata, @NotNull Map<String, Class<?>[]> typeKeys) {
        forEachBindableProperty(metadata, (bv, accessor) -> {
            Type genericType = accessor.getGenericType();
            if (!(genericType instanceof ParameterizedType pt)) return;
            if (!(pt.getRawType() instanceof Class<?> rt) || !Map.class.isAssignableFrom(rt)) return;
            if (pt.getActualTypeArguments().length != 2) return;

            Class<?> keyClass = extractRawClass(pt.getActualTypeArguments()[0]);
            Class<?> valClass = extractRawClass(pt.getActualTypeArguments()[1]);
            if (keyClass == null || valClass == null) return;

            String key = "GsonMap:" + keyClass.getName() + ":" + valClass.getName();
            if (typeKeys.containsKey(key))
                resolveAndUpdate(bv, key);
        });
    }

    private static Class<?> extractRawClass(Type type) {
        if (type instanceof Class<?> c) return c;
        if (type instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> raw) return raw;
        return null;
    }

    /**
     * Looks up a registered {@link BasicType} by key and replaces the
     * {@link BasicValue.Resolution} on the given property via reflection, ensuring
     * Hibernate uses the correct custom type at runtime.
     * <p>
     * Hibernate 6+ eagerly resolves types during {@code metadata.build()} and stores
     * the result in a {@code Resolution} object with final fields. Since
     * {@code Resolution.updateResolution()} does not propagate to all internal fields,
     * this method replaces the entire {@code Resolution} instance on the {@code BasicValue}.
     *
     * @param bv the property's basic value whose resolution should be replaced
     * @param key the registered type key to look up in the basic type registry
     */
    @SuppressWarnings("rawtypes")
    private static void resolveAndUpdate(@NotNull BasicValue bv, @NotNull String key) {
        BasicType<?> resolved = bv.getTypeConfiguration()
            .getBasicTypeRegistry()
            .getRegisteredType(key);

        if (resolved == null) return;

        RESOLUTION_ACCESSOR.set(bv, new BasicValue.Resolution() {
            @Override public BasicType getLegacyResolvedBasicType() { return resolved; }
            @Override public JdbcMapping getJdbcMapping() { return resolved; }
            @Override public JavaType getDomainJavaType() { return resolved.getJavaTypeDescriptor(); }
            @Override public JavaType getRelationalJavaType() { return resolved.getJavaTypeDescriptor(); }
            @Override public JdbcType getJdbcType() { return resolved.getJdbcType(); }
            @Override public BasicValueConverter getValueConverter() { return resolved.getValueConverter(); }
            @Override public MutabilityPlan getMutabilityPlan() { return resolved.getJavaTypeDescriptor().getMutabilityPlan(); }
        });
    }

    /**
     * Iterates all entity property bindings backed by a {@link BasicValue},
     * invoking {@code action} for each eligible property.
     * <p>
     * A property is eligible when:
     * <ul>
     *     <li>Its mapped value is a {@link BasicValue}</li>
     *     <li>The corresponding Java field can be resolved on the entity class</li>
     *     <li>The field is not annotated with {@link Convert @Convert}</li>
     * </ul>
     *
     * @param metadata the built Hibernate metadata to iterate
     * @param action callback receiving the property's {@link BasicValue} and
     *               its resolved {@link FieldAccessor}
     */
    private static void forEachBindableProperty(@NotNull Metadata metadata, @NotNull BiConsumer<BasicValue, FieldAccessor<?>> action) {
        for (PersistentClass pc : metadata.getEntityBindings()) {
            Reflection<?> reflection = new Reflection<>(pc.getMappedClass());

            for (Property property : pc.getProperties()) {
                if (!(property.getValue() instanceof BasicValue bv)) continue;

                FieldAccessor<?> accessor;
                try {
                    accessor = reflection.getField(property.getName());
                } catch (Exception ignored) {
                    continue;
                }

                if (accessor.hasAnnotation(Convert.class)) continue;

                action.accept(bv, accessor);
            }
        }
    }

}

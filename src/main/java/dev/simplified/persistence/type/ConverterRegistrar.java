package dev.simplified.persistence.type;

import com.google.gson.Gson;
import dev.simplified.persistence.JpaModel;
import dev.simplified.reflection.Reflection;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import org.hibernate.boot.MetadataBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Discovers {@link AttributeConverter} implementations annotated with
 * {@link Converter @Converter} in the persistence package tree and registers them
 * with the Hibernate {@link MetadataBuilder}.
 * <p>
 * Hibernate reads the {@link Converter#autoApply()} flag natively - converters marked
 * {@code autoApply = true} are applied globally to all matching-type fields, while others
 * require explicit {@link jakarta.persistence.Convert @Convert} references.
 */
public final class ConverterRegistrar implements TypeRegistrar {

    private final Set<Class<? extends AttributeConverter<?, ?>>> converters = new LinkedHashSet<>();

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void scan(@NotNull Gson gson, @NotNull Iterable<Class<JpaModel>> models) {
        Reflection.getResources()
            .filterPackage(JpaModel.class)
            .getSubtypesOf(AttributeConverter.class)
            .stream()
            .filter(cls -> cls.isAnnotationPresent(Converter.class))
            .forEach(cls -> this.converters.add((Class<? extends AttributeConverter<?, ?>>) cls));
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void register(@NotNull MetadataBuilder builder) {
        this.converters.forEach(cls -> builder.applyAttributeConverter((Class) cls));
    }

}

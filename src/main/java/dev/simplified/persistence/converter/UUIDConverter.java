package dev.simplified.persistence.converter;

import dev.simplified.util.StringUtil;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.util.UUID;

/**
 * JPA {@link AttributeConverter} that converts between {@link UUID} and its string representation.
 * Auto-applied to all UUID fields via {@code @Converter(autoApply = true)}.
 */
@Converter(autoApply = true)
public class UUIDConverter implements AttributeConverter<UUID, String> {

    /** {@inheritDoc} */
    @Override
    public UUID convertToEntityAttribute(String attr) {
        if (StringUtil.isEmpty(attr))
            return null;

        return StringUtil.toUUID(attr);
    }

    /** {@inheritDoc} */
    @Override
    public String convertToDatabaseColumn(UUID attr) {
        try {
            return attr.toString();
        } catch (Exception e) {
            return "";
        }
    }

}

package dev.simplified.persistence.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.awt.*;

/**
 * JPA {@link AttributeConverter} that converts between {@link Color} and its RGB integer
 * string representation for database storage.
 */
@Converter
public class ColorConverter implements AttributeConverter<Color, String> {

    /** {@inheritDoc} */
    @Override
    public Color convertToEntityAttribute(String attr) {
        if (attr == null)
            return null;

        try {
            return Color.getColor(attr);
        } catch (Exception e) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String convertToDatabaseColumn(Color attr) {
        try {
            return String.valueOf(attr.getRGB());
        } catch (Exception e) {
            return "";
        }
    }

}

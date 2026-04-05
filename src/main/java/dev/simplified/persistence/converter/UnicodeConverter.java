package dev.simplified.persistence.converter;

import dev.simplified.util.CharUtil;
import dev.simplified.util.StringUtil;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * JPA {@link AttributeConverter} that converts between {@link Character} and its Unicode hex
 * string representation (without the {@code \\u} prefix) for database storage.
 */
@Converter
public class UnicodeConverter implements AttributeConverter<Character, String> {

    /** {@inheritDoc} */
    @Override
    public Character convertToEntityAttribute(String attr) {
        if (attr == null)
            return null;

        try {
            return CharUtil.toChar(StringUtil.unescapeUnicode(String.format("\\u%s", attr)));
        } catch (Exception e) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String convertToDatabaseColumn(Character attr) {
        try {
            return StringUtil.escapeUnicode(attr.toString())
                .toLowerCase()
                .replaceAll("\\\\u", "");
        } catch (Exception e) {
            return "";
        }
    }

}

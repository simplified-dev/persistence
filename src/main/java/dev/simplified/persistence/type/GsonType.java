package dev.simplified.persistence.type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class for Gson-serialized JSON CLOB storage when used as a Hibernate entity field type.
 * <p>
 * Place this annotation on inner classes that are entirely Gson-represented and should be
 * stored as a JSON column. {@link GsonJsonType.Registrar} automatically discovers annotated
 * types in entity fields and registers a {@link GsonJsonType} per unique class - no per-field
 * {@code @Type} annotation is needed.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface GsonType {

}

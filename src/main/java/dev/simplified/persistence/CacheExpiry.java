package dev.simplified.persistence;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Specifies the auto-refresh interval for a {@link Repository}'s cached data.
 * When placed on a {@link JpaModel} entity class, controls how frequently
 * the repository re-queries the data source. Defaults to 30 seconds if not present.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheExpiry {

    /**
     * Default cache expiry instance used when no {@code @CacheExpiry} annotation
     * is present (30 seconds).
     */
    @NotNull CacheExpiry DEFAULT = new CacheExpiry() {

        @Override
        public Class<? extends Annotation> annotationType() {
            return CacheExpiry.class;
        }

        @Override
        public @NotNull TimeUnit length() {
            return TimeUnit.SECONDS;
        }

        @Override
        public int value() {
            return 30;
        }

    };

    /** The time unit for the expiry interval. */
    @NotNull TimeUnit length();

    /** The numeric value of the expiry interval, in units of {@link #length()}. */
    int value();

}

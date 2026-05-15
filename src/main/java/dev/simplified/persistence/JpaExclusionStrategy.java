package dev.simplified.persistence;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;

/**
 * Gson {@link ExclusionStrategy} that excludes fields annotated with JPA
 * relationship annotations ({@link ManyToOne}, {@link OneToMany}) from
 * serialization.
 *
 * <p>This prevents infinite recursion and unnecessary eager loading when
 * serializing JPA entities to JSON.</p>
 */
public final class JpaExclusionStrategy implements ExclusionStrategy {

    /**
     * Shared singleton instance.
     */
    public static final @NotNull JpaExclusionStrategy INSTANCE = new JpaExclusionStrategy();

    private JpaExclusionStrategy() {}

    @Override
    public boolean shouldSkipField(@NonNull FieldAttributes f) {
        return f.getAnnotation(ManyToOne.class) != null
            || f.getAnnotation(OneToMany.class) != null;
    }

    @Override
    public boolean shouldSkipClass(Class<?> clazz) {
        return false;
    }

}

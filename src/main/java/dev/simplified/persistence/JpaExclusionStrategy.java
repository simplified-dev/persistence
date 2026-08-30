package dev.simplified.persistence;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import org.jetbrains.annotations.NotNull;
import org.jspecify.annotations.NonNull;

/**
 * Gson {@link ExclusionStrategy} that excludes a field holding rows another document owns.
 *
 * <p>A row carries the id of what it points at and never the thing itself, so a field that resolves
 * to entities is absent from the wire in both directions. Including one would recurse on the way out
 * and would fail to bind an id to an object on the way in.
 *
 * <p>{@link Linked} is the declaration that says so. {@link ManyToOne} and {@link OneToMany} are
 * recognised alongside it while models still carry them.
 */
public final class JpaExclusionStrategy implements ExclusionStrategy {

    /**
     * Shared singleton instance.
     */
    public static final @NotNull JpaExclusionStrategy INSTANCE = new JpaExclusionStrategy();

    private JpaExclusionStrategy() {}

    @Override
    public boolean shouldSkipField(@NonNull FieldAttributes f) {
        return f.getAnnotation(Linked.class) != null
            || f.getAnnotation(ManyToOne.class) != null
            || f.getAnnotation(OneToMany.class) != null;
    }

    @Override
    public boolean shouldSkipClass(Class<?> clazz) {
        return false;
    }

}

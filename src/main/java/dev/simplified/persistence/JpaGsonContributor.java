package dev.simplified.persistence;

import dev.simplified.gson.GsonContributor;
import dev.simplified.gson.GsonSettings;
import org.jetbrains.annotations.NotNull;

import java.util.ServiceLoader;

/**
 * Registers {@link JpaExclusionStrategy} with {@link GsonSettings#defaults()}.
 *
 * <p>Discovered through the {@link ServiceLoader} entry at
 * {@code META-INF/services/dev.simplified.gson.GsonContributor} whenever this module is on the
 * classpath, so a row is never serialized with its links resolved into it no matter which instance
 * writes it. {@link Linked} is a persistence annotation, so the registration that enforces it
 * belongs beside it rather than in whichever consumer happened to declare the first linked field.
 */
public final class JpaGsonContributor implements GsonContributor {

    /** {@inheritDoc} */
    @Override
    public void contribute(GsonSettings.@NotNull Builder builder) {
        builder.withExclusionStrategies(JpaExclusionStrategy.INSTANCE);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Applied after default-priority contributors so the exclusion sees the full registered
     * type-adapter set.
     */
    @Override
    public int priority() {
        return 100;
    }

}

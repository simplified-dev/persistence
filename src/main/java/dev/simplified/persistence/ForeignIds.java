package dev.simplified.persistence;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Links a transient collection field to its companion ID list field for
 * automatic resolution after entities are loaded from the database.
 * <p>
 * When an entity has a {@code ConcurrentList<String>} field containing foreign
 * IDs and a corresponding transient {@code ConcurrentList<TargetEntity>} field,
 * this annotation tells {@link JpaRepository} to resolve the IDs into entities
 * via repository lookups after each query.
 *
 * <pre>{@code
 * @Column(name = "region_ids")
 * private @NotNull ConcurrentList<String> regionIds = Concurrent.newList();
 *
 * @ForeignIds("regionIds")
 * private transient @NotNull ConcurrentList<Region> regions = Concurrent.newList();
 * }</pre>
 *
 * @see JpaRepository
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ForeignIds {

    /**
     * The name of the companion {@code ConcurrentList<String>} field containing the foreign IDs.
     */
    @NotNull String value();

}

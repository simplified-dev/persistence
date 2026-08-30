package dev.simplified.persistence;

import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field as holding whatever the named id property points at.
 *
 * <p>The field's own type decides the shape: a collection resolves many and anything else resolves
 * one, with the target read from the element type or from the field type respectively. So a link to
 * one row and a link to many rows are one declaration rather than two mechanisms.
 *
 * <pre>{@code
 * @Column(name = "category_id")
 * private @NotNull String categoryId = "";
 *
 * @Linked("categoryId")
 * private transient @NotNull ItemCategory category;
 *
 * @Column(name = "item_ids")
 * private @NotNull ConcurrentList<String> itemIds = Concurrent.newList();
 *
 * @Linked("itemIds")
 * private transient @NotNull ConcurrentList<Item> items = Concurrent.newList();
 * }</pre>
 *
 * <p>Resolution runs once per generation, before it is published, so a reader never pays for it and an
 * index built over the generation describes rows that are already whole.
 *
 * <p>A marked field is also excluded from serialization. The two jobs travel together because
 * separating them writes a resolved graph back to the origin: the field holds rows another document
 * owns, and the document this row belongs to carries only the id.
 *
 * @see JpaExclusionStrategy
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Linked {

    /**
     * The name of the property carrying the id or ids to resolve.
     */
    @NotNull String value();

}

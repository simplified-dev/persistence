package dev.simplified.persistence;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;

/**
 * Marks a field as holding whatever the named id property points at.
 *
 * <p>The field's own type decides the shape, and what an id naming no row does:
 * <ul>
 *     <li><b>a list</b> - {@code ConcurrentList<X>}, or a type a {@link ConcurrentList} can be
 *         assigned to - resolves many, and an id naming no row drops out</li>
 *     <li><b>an {@link Optional}</b> - {@code Optional<X>} - resolves one, and holds empty when the id
 *         is absent or names no row</li>
 *     <li><b>anything else</b> - {@code X} - resolves one, and an absent id or one naming no row fails
 *         the rebuild with a {@link JpaException} naming the field, the type and the id</li>
 * </ul>
 * So a link to one row, to one row that may be missing and to many rows are one declaration rather
 * than three mechanisms. {@code X} must be a model class itself, not a wildcard or a type variable;
 * any other shape is refused at connect, before anything is read.
 *
 * <pre>{@code
 * @Column(name = "category_id")
 * private @NotNull String categoryId = "";
 *
 * @Linked("categoryId")
 * private transient @NotNull ItemCategory category;
 *
 * @Column(name = "stone_id")
 * private @NotNull Optional<String> stoneId = Optional.empty();
 *
 * @Linked("stoneId")
 * private transient @NotNull Optional<Item> stone = Optional.empty();
 *
 * @Column(name = "item_ids")
 * private @NotNull ConcurrentList<String> itemIds = Concurrent.newList();
 *
 * @Linked("itemIds")
 * private transient @NotNull ConcurrentList<Item> items = Concurrent.newList();
 * }</pre>
 *
 * <p>A miss on a field of the third shape fails the whole rebuild - every type it covers publishes
 * nothing - and at connect that is every registered type. {@link JpaSession#write} refuses an upsert
 * whose rows would miss before it reaches the source, but a delete of a row other rows still name
 * lands, and fails every rebuild covering them and every connect after it until the data is
 * repaired. The upsert is checked against the rows the session holds, with the request's own rows
 * added to the written type's, so two new rows of different types naming each other through fields
 * of the third shape cannot be written; one side has to be an {@link Optional}, or first name a row
 * that is already held.
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

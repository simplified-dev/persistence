package dev.simplified.persistence;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.reflection.Reflection;
import dev.simplified.reflection.accessor.FieldAccessor;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Root marker interface for every model the persistence layer holds.
 *
 * <p>A model already declares the two things everything else needs to find it: the table it is
 * published as, and the property its rows are keyed on. Those are read here so there is one answer
 * to each - a document source, a write and an index that resolved the key separately would be three
 * accessors from three sources, and that is how a read key and a write key diverge.
 *
 * <p>Extends {@link Serializable} to support Hibernate session serialization and L2 cache storage.
 */
@SuppressWarnings("all")
public interface JpaModel extends Serializable {

    /**
     * The logical document a type is published as, which is the table it declares.
     *
     * @param type the model class
     * @return the logical name
     * @throws JpaException if the type declares no table name
     */
    static @NotNull String documentOf(@NotNull Class<? extends JpaModel> type) {
        Table table = type.getAnnotation(Table.class);

        if (table == null || table.name().isEmpty())
            throw new JpaException("'%s' names no table, so it names no document", type.getName());

        return table.name();
    }

    /**
     * The property a type's rows are keyed on.
     *
     * @param type the model class
     * @return the accessor for the id field
     * @throws JpaException if the type declares no id
     */
    static @NotNull FieldAccessor<?> keyOf(@NotNull Class<? extends JpaModel> type) {
        return new Reflection<>(type).getFields()
            .stream()
            .filter(field -> field.hasAnnotation(Id.class))
            .findFirst()
            .orElseThrow(() -> new JpaException("'%s' declares no id", type.getName()));
    }

    /**
     * Indexes rows by their key, in the order they arrive.
     *
     * <p>A repeated key replaces the row in place rather than appending a second one, which is what
     * makes a later document layer an override of an earlier one.
     *
     * @param type the model class
     * @param rows the rows to index
     * @param <T> the entity type
     * @return the rows keyed by their stringified id
     * @throws JpaException if the type declares no id, or a row carries none
     */
    static <T extends JpaModel> @NotNull ConcurrentMap<String, T> keyed(
        @NotNull Class<T> type,
        @NotNull Iterable<T> rows
    ) {
        FieldAccessor<?> key = keyOf(type);
        ConcurrentMap<String, T> keyed = Concurrent.newLinkedMap();

        for (T row : rows) {
            Object id = key.get(row);

            if (id == null)
                throw new JpaException("A row of '%s' carries no id, so nothing can name it", type.getName());

            keyed.put(String.valueOf(id), row);
        }

        return keyed;
    }

}

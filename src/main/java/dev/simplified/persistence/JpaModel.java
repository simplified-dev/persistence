package dev.simplified.persistence;

import java.io.Serializable;

/**
 * Root marker interface for all JPA entity models in the persistence layer.
 * Extends {@link Serializable} to support Hibernate session serialization
 * and L2 cache storage.
 */
@SuppressWarnings("all")
public interface JpaModel extends Serializable {

}
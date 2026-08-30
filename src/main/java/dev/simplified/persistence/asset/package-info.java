/**
 * JPA entities that persist change-detection state for the external origins an
 * {@link dev.simplified.persistence.store.Source Source} loads from.
 *
 * <p>The package is deliberately isolated from {@code dev.simplified.persistence} so that existing
 * {@code RepositoryFactory} callers, which anchor their model scan under their own package trees,
 * do not accidentally register these tables.
 *
 * <p>Consumers that want asset state tracked in their session explicitly add
 * {@code .withPackageOf(ExternalAssetState.class)} to a secondary
 * {@code RepositoryFactory.builder()} or include the types via a custom factory implementation.
 *
 * @see dev.simplified.persistence.store.Source
 */
package dev.simplified.persistence.asset;

/**
 * JPA entities that persist change-detection state for external asset sources such as
 * {@link dev.simplified.persistence.source.RemoteJsonSource}.
 *
 * <p>Phase 4a ships the schema only. Phase 4c wires these entities into a scheduled
 * poller that computes manifest deltas and drives incremental refresh of downstream
 * repositories. The package is deliberately isolated from
 * {@code dev.simplified.persistence} so that existing {@code RepositoryFactory}
 * callers (which anchor their model scan under their own package trees) do not
 * accidentally register these tables.
 *
 * <p>Consumers that want asset state tracked in their session explicitly add
 * {@code .withPackageOf(ExternalAssetState.class)} to a secondary
 * {@code RepositoryFactory.builder()} or include the types via a custom factory
 * implementation. Phase 4c ships the first such wiring inside {@code simplified-data}.
 *
 * @see dev.simplified.persistence.source.RemoteJsonSource
 */
package dev.simplified.persistence.asset;

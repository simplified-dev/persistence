package dev.simplified.persistence;

import com.google.gson.Gson;
import dev.simplified.annotations.Getter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.store.DocumentOrigin;
import dev.simplified.persistence.store.Source;
import dev.simplified.persistence.unmapped.ContractRow;
import dev.simplified.persistence.unmapped.LayeredRow;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Covers a factory read through the {@link RepositoryFactory} type rather than through the class
 * that declares it.
 *
 * <p>A consumer writes its factory as annotated fields and never writes the accessors, so whether an
 * accessor overrides the contract or merely sits beside it is decided by a generated name. A name
 * that drifts from the contract's does not fail to compile where the contract has a default - the
 * default answers instead, quietly, and a test holding the concrete class still sees the field.
 * Holding the interface is what makes that visible. The drift has landed four times in this
 * workspace and never had a guard.
 */
class RepositoryFactoryContractTest {

    /**
     * An origin publishing nothing, so a split factory has a second source that is not its first.
     */
    private static final @NotNull DocumentOrigin EMPTY_ORIGIN = new DocumentOrigin() {

        @Override
        public @NotNull ConcurrentList<String> layersOf(@NotNull String name) {
            return Concurrent.newUnmodifiableList();
        }

        @Override
        public @NotNull String read(@NotNull String path) {
            return "[]";
        }

    };

    /**
     * A factory written the way a consumer writes one: fields, an annotation, no accessors.
     */
    @Getter
    private static final class DeclaredFactory implements RepositoryFactory {

        private final @NotNull ConcurrentList<Class<JpaModel>> models;
        private final @NotNull Source source;

        private DeclaredFactory(@NotNull ConcurrentList<Class<JpaModel>> models, @NotNull Source source) {
            this.models = models;
            this.source = source;
        }

    }

    /**
     * A factory drawing on more than one origin, which is the only reason to override
     * {@link RepositoryFactory#sourceFor(Class)}.
     */
    private static final class SplitFactory implements RepositoryFactory {

        private final @NotNull Source primary = Source.none();
        private final @NotNull Source secondary = Source.documents(EMPTY_ORIGIN, new Gson());

        @Override
        public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
            return Concurrent.newUnmodifiableList();
        }

        @Override
        public @NotNull Source getSource() {
            return this.primary;
        }

        @Override
        public @NotNull Source sourceFor(@NotNull Class<? extends JpaModel> type) {
            return type == LayeredRow.class ? this.secondary : this.primary;
        }

    }

    @SuppressWarnings("unchecked")
    private static @NotNull ConcurrentList<Class<JpaModel>> models(@NotNull Class<?>... types) {
        ConcurrentList<Class<JpaModel>> listed = Concurrent.newList();

        for (Class<?> type : types)
            listed.add((Class<JpaModel>) type);

        return listed.toUnmodifiable();
    }

    @Test
    @DisplayName("a declared factory answers its own fields through the contract type")
    void generatedAccessorsSatisfyTheContract() {
        Source declared = Source.none();

        // Held as the interface deliberately: a generated accessor whose name drifted would leave
        // the contract's own answer in place, and only this reference sees that.
        RepositoryFactory factory = new DeclaredFactory(models(ContractRow.class, LayeredRow.class), declared);

        assertThat(factory.getModels(), contains(ContractRow.class, LayeredRow.class));
        assertThat(factory.getSource(), sameInstance(declared));
    }

    @Test
    @DisplayName("sourceFor falls through to the single source unless a factory says otherwise")
    void sourceForDefaultsToTheOneSource() {
        Source declared = Source.none();
        RepositoryFactory factory = new DeclaredFactory(models(ContractRow.class), declared);

        assertThat(factory.sourceFor(ContractRow.class), sameInstance(declared));
        assertThat(factory.sourceFor(LayeredRow.class), sameInstance(declared));
    }

    @Test
    @DisplayName("a factory over more than one origin routes each type to its own")
    void sourceForRoutesPerType() {
        RepositoryFactory factory = new SplitFactory();

        assertThat(factory.sourceFor(ContractRow.class), sameInstance(factory.getSource()));
        assertThat(factory.sourceFor(LayeredRow.class), is(not(factory.getSource())));
    }

    @Test
    @DisplayName("the anchored factory scans the anchor's package and holds no origin")
    void anchoredFactoryScansAndHoldsNone() {
        RepositoryFactory factory = RepositoryFactory.of(ContractRow.class);

        assertThat(factory.getSource(), sameInstance(Source.none()));
        assertThat(factory.getModels().contains(ContractRow.class), is(true));
        assertThat(factory.getModels().contains(LayeredRow.class), is(true));
    }

    @Test
    @DisplayName("the anchored factory carries the origin it was given to every type")
    void anchoredFactoryCarriesItsOrigin() {
        Source declared = Source.none();
        RepositoryFactory factory = RepositoryFactory.of(ContractRow.class, declared);

        assertThat(factory.getSource(), sameInstance(declared));
        assertThat(factory.sourceFor(ContractRow.class), sameInstance(declared));
    }

    @Test
    @DisplayName("discovery is stable, so two scans of one anchor agree")
    void discoveryIsStable() {
        assertThat(
            RepositoryFactory.resolveModels(ContractRow.class),
            equalTo(RepositoryFactory.resolveModels(ContractRow.class))
        );
    }

}

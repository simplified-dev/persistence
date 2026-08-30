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

import java.util.Optional;

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
        private final @NotNull Optional<Source> source;

        private DeclaredFactory(@NotNull ConcurrentList<Class<JpaModel>> models, @NotNull Optional<Source> source) {
            this.models = models;
            this.source = source;
        }

    }

    /**
     * A factory drawing on more than one origin, which is the only reason to override
     * {@link RepositoryFactory#sourceFor(Class)}.
     */
    private static final class SplitFactory implements RepositoryFactory {

        private final @NotNull Optional<Source> primary = Optional.empty();
        private final @NotNull Optional<Source> secondary = Optional.of(Source.documents(EMPTY_ORIGIN, new Gson()));

        @Override
        public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
            return Concurrent.newUnmodifiableList();
        }

        @Override
        public @NotNull Optional<Source> getSource() {
            return this.primary;
        }

        @Override
        public @NotNull Optional<Source> sourceFor(@NotNull Class<? extends JpaModel> type) {
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
        Source declared = Source.documents(EMPTY_ORIGIN, new Gson());

        // Held as the interface deliberately: a generated accessor whose name drifted would leave
        // the contract's own answer in place, and only this reference sees that.
        RepositoryFactory factory = new DeclaredFactory(models(ContractRow.class, LayeredRow.class), Optional.of(declared));

        assertThat(factory.getModels(), contains(ContractRow.class, LayeredRow.class));
        assertThat(factory.getSource().orElseThrow(), sameInstance(declared));
    }

    @Test
    @DisplayName("sourceFor falls through to the single source unless a factory says otherwise")
    void sourceForDefaultsToTheOneSource() {
        Source declared = Source.documents(EMPTY_ORIGIN, new Gson());
        RepositoryFactory factory = new DeclaredFactory(models(ContractRow.class), Optional.of(declared));

        assertThat(factory.sourceFor(ContractRow.class).orElseThrow(), sameInstance(declared));
        assertThat(factory.sourceFor(LayeredRow.class).orElseThrow(), sameInstance(declared));
    }

    @Test
    @DisplayName("a factory over more than one origin routes each type to its own")
    void sourceForRoutesPerType() {
        RepositoryFactory factory = new SplitFactory();

        assertThat(factory.sourceFor(ContractRow.class), equalTo(factory.getSource()));
        assertThat(factory.sourceFor(LayeredRow.class), is(not(factory.getSource())));
    }

    @Test
    @DisplayName("the anchored factory scans the anchor's package and names no origin")
    void anchoredFactoryScansAndNamesNoOrigin() {
        RepositoryFactory factory = RepositoryFactory.of(ContractRow.class);

        // Empty is not "no rows" - it says the session's database authors them, and a session that
        // opened none fails rather than serving an empty repository.
        assertThat(factory.getSource().isEmpty(), is(true));
        assertThat(factory.sourceFor(ContractRow.class).isEmpty(), is(true));
        assertThat(factory.getModels().contains(ContractRow.class), is(true));
        assertThat(factory.getModels().contains(LayeredRow.class), is(true));
    }

    @Test
    @DisplayName("the anchored factory carries the origin it was given to every type")
    void anchoredFactoryCarriesItsOrigin() {
        Source declared = Source.documents(EMPTY_ORIGIN, new Gson());
        RepositoryFactory factory = RepositoryFactory.of(ContractRow.class, declared);

        assertThat(factory.getSource().orElseThrow(), sameInstance(declared));
        assertThat(factory.sourceFor(ContractRow.class).orElseThrow(), sameInstance(declared));
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

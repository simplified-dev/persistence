package dev.simplified.persistence.store;

import dev.simplified.collection.Concurrent;
import dev.simplified.persistence.model.TestParentModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the shape of a write instruction: what it carries, and what it refuses to let a caller
 * change after the fact.
 */
class WriteRequestTest {

    private static TestParentModel row(int id, String name) {
        TestParentModel model = new TestParentModel();
        model.setId(id);
        model.setName(name);
        return model;
    }

    @Test
    @DisplayName("an upsert names its type, its operation and its rows")
    void upsertCarriesTypeOperationAndRows() {
        TestParentModel first = row(1, "first");
        TestParentModel second = row(2, "second");

        WriteRequest<TestParentModel> request = WriteRequest.upsert(TestParentModel.class, List.of(first, second));

        assertThat(request.type(), equalTo(TestParentModel.class));
        assertThat(request.operation(), equalTo(WriteRequest.Operation.UPSERT));
        assertThat(request.rows(), contains(first, second));
    }

    @Test
    @DisplayName("a delete carries the whole row rather than only its key")
    void deleteCarriesTheWholeRow() {
        TestParentModel only = row(1, "first");

        WriteRequest<TestParentModel> request = WriteRequest.delete(TestParentModel.class, List.of(only));

        assertThat(request.operation(), equalTo(WriteRequest.Operation.DELETE));
        assertThat(request.rows(), contains(only));
        assertThat(request.rows().getFirst().getName(), equalTo("first"));
    }

    @Test
    @DisplayName("a request applies unconditionally until a precondition is named")
    void preconditionIsAbsentUntilNamed() {
        WriteRequest<TestParentModel> unconditional = WriteRequest.upsert(TestParentModel.class, List.of(row(1, "first")));

        assertThat(unconditional.getPrecondition().isPresent(), is(false));
        assertThat(unconditional.getPrecondition().isEmpty(), is(true));
    }

    @Test
    @DisplayName("expecting returns a copy and leaves the original unconditional")
    void expectingCopiesRatherThanMutates() {
        WriteRequest<TestParentModel> unconditional = WriteRequest.upsert(TestParentModel.class, List.of(row(1, "first")));
        WriteRequest<TestParentModel> conditional = unconditional.expecting("e3ac8cc");

        assertThat(conditional.getPrecondition().orElseThrow(), equalTo("e3ac8cc"));
        assertThat(unconditional.getPrecondition().isEmpty(), is(true));

        // everything else travels across unchanged
        assertThat(conditional.type(), equalTo(unconditional.type()));
        assertThat(conditional.operation(), equalTo(unconditional.operation()));
        assertThat(conditional.rows(), equalTo(unconditional.rows()));
    }

    @Test
    @DisplayName("the rows a request carries cannot be added to afterwards")
    void rowsAreSealed() {
        WriteRequest<TestParentModel> request = WriteRequest.upsert(TestParentModel.class, List.of(row(1, "first")));

        assertThrows(UnsupportedOperationException.class, () -> request.rows().add(row(2, "second")));
    }

    @Test
    @DisplayName("a request over no rows is legal and empty")
    void emptyRowsAreLegal() {
        WriteRequest<TestParentModel> request = WriteRequest.upsert(TestParentModel.class, Concurrent.newList());

        assertThat(request.rows().isEmpty(), is(true));
        assertThat(request.operation(), equalTo(WriteRequest.Operation.UPSERT));
    }

}

package dev.simplified.persistence.source;

import dev.simplified.collection.Concurrent;
import dev.simplified.persistence.unmapped.ContractRow;
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

    private static ContractRow row(int id, String name) {
        ContractRow model = new ContractRow();
        model.setId(id);
        model.setName(name);
        return model;
    }

    @Test
    @DisplayName("an upsert names its type, its operation and its rows")
    void upsertCarriesTypeOperationAndRows() {
        ContractRow first = row(1, "first");
        ContractRow second = row(2, "second");

        WriteRequest<ContractRow> request = WriteRequest.upsert(ContractRow.class, List.of(first, second));

        assertThat(request.type(), equalTo(ContractRow.class));
        assertThat(request.operation(), equalTo(WriteRequest.Operation.UPSERT));
        assertThat(request.rows(), contains(first, second));
    }

    @Test
    @DisplayName("a delete carries the whole row rather than only its key")
    void deleteCarriesTheWholeRow() {
        ContractRow only = row(1, "first");

        WriteRequest<ContractRow> request = WriteRequest.delete(ContractRow.class, List.of(only));

        assertThat(request.operation(), equalTo(WriteRequest.Operation.DELETE));
        assertThat(request.rows(), contains(only));
        assertThat(request.rows().getFirst().getName(), equalTo("first"));
    }

    @Test
    @DisplayName("the rows a request carries cannot be added to afterwards")
    void rowsAreSealed() {
        WriteRequest<ContractRow> request = WriteRequest.upsert(ContractRow.class, List.of(row(1, "first")));

        assertThrows(UnsupportedOperationException.class, () -> request.rows().add(row(2, "second")));
    }

    @Test
    @DisplayName("a request over no rows is legal and empty")
    void emptyRowsAreLegal() {
        WriteRequest<ContractRow> request = WriteRequest.upsert(ContractRow.class, Concurrent.newList());

        assertThat(request.rows().isEmpty(), is(true));
        assertThat(request.operation(), equalTo(WriteRequest.Operation.UPSERT));
    }

}

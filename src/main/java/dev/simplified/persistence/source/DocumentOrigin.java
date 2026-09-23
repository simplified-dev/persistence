package dev.simplified.persistence.source;

import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

/**
 * A tree of files a document source draws its layers out of.
 *
 * <p>Three questions describe one: which paths a logical name is made of, what the text at a path
 * is, and - for an origin a caller holds instructions to update - what the text at a path becomes.
 * A GitHub repository, a directory on disk and a bucket all answer them, and none of them has to
 * share a catalogue type with any other, because the answer is ordered paths rather than a
 * catalogue.
 *
 * <p>Reading is all an origin promises. Writing is {@link Writable}, and an origin that was handed
 * no write instruction simply is not one.
 *
 * @see DocumentSource
 * @see WritableDocumentSource
 */
public interface DocumentOrigin {

    /**
     * The ordered paths a logical document is made of.
     *
     * <p>Order is merge order: a later path repeating a key replaces the row an earlier one carried,
     * which is what makes a companion file an override of the file it accompanies rather than a
     * second copy of it.
     *
     * @param name the logical document name
     * @return the paths in merge order, empty when the origin publishes no such document
     * @throws JpaException if the origin cannot be asked
     */
    @NotNull ConcurrentList<String> layersOf(@NotNull String name) throws JpaException;

    /**
     * Reads the text at one path.
     *
     * @param path a path this origin published, relative to its root
     * @return the content as a string
     * @throws JpaException if the path cannot be read or decoded
     */
    @NotNull String read(@NotNull String path) throws JpaException;

    /**
     * The write half, for an origin a caller holds instructions to update.
     */
    interface Writable extends DocumentOrigin {

        /**
         * Replaces the text at one path.
         *
         * <p>The precondition is whatever token this origin recognises for "the path is still as I
         * last saw it" - a blob sha, a revision, a content hash. Named, it is sent and a moved path
         * refuses the write; absent, the origin resolves its own, which still refuses a write over a
         * path that moved.
         *
         * @param path a path this origin published, relative to its root
         * @param content the text to write
         * @param precondition the token the caller expects the path to still carry, empty to let the
         *        origin resolve one
         * @throws JpaException if the write fails, including when the precondition no longer holds
         */
        void write(
            @NotNull String path,
            @NotNull String content,
            @NotNull Optional<String> precondition
        ) throws JpaException;

    }

}

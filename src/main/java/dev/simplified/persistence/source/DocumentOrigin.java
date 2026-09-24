package dev.simplified.persistence.source;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaSession;
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
 * catalogue. An origin that can also say which documents moved answers
 * {@link #fingerprints()}, which spares a session the reads that would find nothing new.
 *
 * <p>Reading is all an origin promises. Writing is {@link Writable}, and an origin that was handed
 * no write instruction simply is not one.
 *
 * @see DocumentSource
 * @see DocumentSource.Writable
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
     * The fingerprint of every document this origin publishes, as it stands now.
     *
     * <p>A fingerprint covers every layer of its document, so a change to any of them moves it, and
     * two answers carrying the same fingerprint for a name describe the same text. A session asks
     * before it reads and skips a document whose fingerprint has not moved since. An origin that
     * cannot fingerprint answers empty, and a document the answer leaves out is read as though it
     * moved.
     *
     * @return the fingerprints keyed by logical document name, empty when this origin cannot
     *         fingerprint
     * @throws JpaException if the origin cannot be asked
     */
    default @NotNull ConcurrentMap<String, String> fingerprints() throws JpaException {
        return Concurrent.newUnmodifiableMap();
    }

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
         * <p>Replacing a file here reaches no session reading this origin. A registered type is
         * written through {@link JpaSession#write(WriteRequest)}, which rebuilds it.
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

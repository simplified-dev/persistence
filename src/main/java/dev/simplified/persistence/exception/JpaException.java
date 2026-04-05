package dev.simplified.persistence.exception;

import org.intellij.lang.annotations.PrintFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Thrown when the persistence layer encounters a configuration, session, or
 * repository error.
 */
public class JpaException extends RuntimeException {

    /**
     * Constructs a new {@code JpaException} with the specified cause.
     *
     * @param cause the underlying throwable that caused this exception
     */
    public JpaException(@NotNull Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code JpaException} with the specified detail message.
     *
     * @param message the detail message
     */
    public JpaException(@NotNull String message) {
        super(message);
    }

    /**
     * Constructs a new {@code JpaException} with the specified cause and detail message.
     *
     * @param cause the underlying throwable that caused this exception
     * @param message the detail message
     */
    public JpaException(@NotNull Throwable cause, @NotNull String message) {
        super(message, cause);
    }

    /**
     * Constructs a new {@code JpaException} with a formatted detail message.
     *
     * @param message the format string
     * @param args the format arguments
     */
    public JpaException(@NotNull @PrintFormat String message, @Nullable Object... args) {
        super(String.format(message, args));
    }

    /**
     * Constructs a new {@code JpaException} with the specified cause and a formatted detail message.
     *
     * @param cause the underlying throwable that caused this exception
     * @param message the format string
     * @param args the format arguments
     */
    public JpaException(@NotNull Throwable cause, @NotNull @PrintFormat String message, @Nullable Object... args) {
        super(String.format(message, args), cause);
    }

}

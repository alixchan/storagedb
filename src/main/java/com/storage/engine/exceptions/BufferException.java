package com.storage.engine.exceptions;

/**
 * The {@code BufferException} class represents an exception that is thrown when a buffer operation fails.
 * It extends the {@link StorageDBRuntimeException} class, which is a runtime exception indicating a
 * general storage DB-related error.
 *
 * <p>This exception is typically thrown when a buffer is accessed in a read-only mode.
 * The exception message provides information about the buffer being read-only.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try {
 *     // Perform buffer operations
 * } catch (BufferException ex) {
 *     // Handle buffer exception
 * }
 * }</pre>
 */
public class BufferException extends StorageDBRuntimeException {
    /**
     * Constructs a new {@code BufferException} object with the specified error message.
     *
     * <p>The error message is appended with a custom message indicating that the buffer is read-only.</p>
     *
     * @param s the error message
     */
    public BufferException(String s) {
        super(s + "\n It's buffer - only-read. ");
    }
}



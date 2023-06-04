package com.storage.engine.exceptions;

/**
 * The StorageDBRuntimeException class represents a runtime exception specific to the StorageDB library.
 * It extends the RuntimeException class and provides additional constructors for capturing error messages and causes.
 *
 * @see RuntimeException
 * @since 1.0
 */
public class StorageDBRuntimeException extends RuntimeException {

    /**
     * Constructs a new StorageDBRuntimeException with no detail message.
     */
    public StorageDBRuntimeException() {
    }

    /**
     * Constructs a new StorageDBRuntimeException with the specified detail message.
     *
     * @param message the detail message describing the specific exception
     */
    public StorageDBRuntimeException(String message) {
        super(message);
    }

    /**
     * Constructs a new StorageDBRuntimeException with the specified cause and a detail message of
     * (cause==null ? null : cause.toString()) (which typically contains the class and detail message of cause).
     *
     * @param cause the cause (which is saved for later retrieval by the getCause() method)
     * @see Throwable#toString()
     */
    public StorageDBRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new StorageDBRuntimeException with the specified detail message and cause.
     *
     * @param message the detail message describing the specific exception
     * @param cause   the cause (which is saved for later retrieval by the getCause() method)
     * @see Throwable#toString()
     */
    public StorageDBRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}


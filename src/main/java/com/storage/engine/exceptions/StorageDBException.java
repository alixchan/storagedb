package com.storage.engine.exceptions;

/**
 * The StorageDBException class is the base exception class for all exceptions related to the StorageDB
 * system.
 * It extends the Exception class.
 */
public class StorageDBException extends Exception {
    /**
     * Constructs a new StorageDBException with the specified error message.
     *
     * @param message the error message describing the specific exception
     */
    public StorageDBException(String message) {
        super(message);
    }
}

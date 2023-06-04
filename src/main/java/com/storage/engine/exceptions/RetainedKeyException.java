package com.storage.engine.exceptions;

/**
 * The {@code RetainedKeyException} class is an unchecked exception that indicates a retained key.
 * It is a subclass of the RuntimeException class.
 */
public class RetainedKeyException extends RuntimeException {
    /**
     * Constructs a new {@code RetainedKeyException} with the specified key.
     *
     * @param key the key that is retained
     * @throws IllegalArgumentException if the key is negative
     */
    public RetainedKeyException(final int key) {
        super("Key 0x" + Integer.toHexString(key) + " retained.");
    }
}

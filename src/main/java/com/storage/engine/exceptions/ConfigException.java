package com.storage.engine.exceptions;

/**
 * This exception is thrown when there is an error in the configuration.
 * It is a subclass of the StorageDBRuntimeException class.
 */
public class ConfigException extends StorageDBRuntimeException {

    /**
     * Constructs a new ConfigException with the specified error message.
     *
     * @param message the error message describing the specific configuration error
     */
    public ConfigException(String message) {
        super(message);
    }
}

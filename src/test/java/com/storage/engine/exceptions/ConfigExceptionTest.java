package com.storage.engine.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigExceptionTest {

    @Test
    void constructor_withMessage_setsMessage() {
        String message = "Invalid configuration";
        ConfigException exception = new ConfigException(message);
        assertEquals(message, exception.getMessage());
    }

    @Test
    void constructor_withNullMessage_setsNullMessage() {
        String message = null;
        ConfigException exception = new ConfigException(message);
        assertEquals(null, exception.getMessage());
    }
}

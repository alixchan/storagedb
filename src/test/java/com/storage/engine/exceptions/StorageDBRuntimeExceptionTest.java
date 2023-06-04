package com.storage.engine.exceptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StorageDBRuntimeExceptionTest {

    @Test
    void constructor_noArguments_setsNullMessageAndCause() {
        StorageDBRuntimeException exception = new StorageDBRuntimeException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void constructor_messageArgument_setsMessageAndNullCause() {
        String message = "Test Exception";
        StorageDBRuntimeException exception = new StorageDBRuntimeException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void constructor_causeArgument_setsNullMessageAndCause() {
        Throwable cause = new IllegalArgumentException();
        StorageDBRuntimeException exception = new StorageDBRuntimeException(cause);
        assertEquals(cause, exception.getCause());
    }

    @Test
    void constructor_messageAndCauseArguments_setsMessageAndCause() {
        String message = "Test Exception";
        Throwable cause = new IllegalArgumentException();
        StorageDBRuntimeException exception = new StorageDBRuntimeException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}

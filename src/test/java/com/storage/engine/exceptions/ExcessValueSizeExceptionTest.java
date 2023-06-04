package com.storage.engine.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExcessValueSizeExceptionTest {

    @Test
    void exceptionMessage_defaultConstructor() {
        ExcessValueSizeException exception = new ExcessValueSizeException();
        String message = exception.getMessage();
        assertEquals(null, message);
    }


    @Test
    void exceptionThrown() {
        assertThrows(ExcessValueSizeException.class, this::methodThatThrowsException);
    }

    private void methodThatThrowsException() {
        throw new ExcessValueSizeException();
    }
}

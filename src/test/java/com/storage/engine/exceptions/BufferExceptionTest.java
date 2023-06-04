package com.storage.engine.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BufferExceptionTest {

    @Test
    void bufferException_constructorWithMessage_correctMessage() {
        String errorMessage = "Error message";
        BufferException exception = new BufferException(errorMessage);
        assertEquals(errorMessage + "\n It's buffer - only-read. ", exception.getMessage());
    }

    @Test
    void bufferException_constructorWithNullMessage_correctMessage() {
        BufferException exception = new BufferException(null);
        assertEquals(null + "\n It's buffer - only-read. ", exception.getMessage());
    }
}

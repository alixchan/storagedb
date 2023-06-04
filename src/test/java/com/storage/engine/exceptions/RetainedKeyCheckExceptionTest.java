package com.storage.engine.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetainedKeyCheckExceptionTest {

    @Test
    void test_message_retainedKeyContains() {
        assertEquals(true, (new RetainedKeyException(Integer.MAX_VALUE))
                .getMessage().contains("0x7fffffff"));
    }
}
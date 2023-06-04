package com.storage.engine.exceptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IncoherentDataExceptionTest {

    private static void execute() {
        throw new IncoherentDataException();
    }

    @Test
    void test_constructor() {
        Assertions.<IncoherentDataException>assertThrows(IncoherentDataException.class, IncoherentDataExceptionTest::execute);
    }
}
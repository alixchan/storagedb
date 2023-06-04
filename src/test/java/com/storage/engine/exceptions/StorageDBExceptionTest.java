package com.storage.engine.exceptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StorageDBExceptionTest {

    private static void execute() throws StorageDBException {
        throw new StorageDBException("exception");
    }

    @Test
    void test_constructor() {
        Assertions.<StorageDBException>assertThrows(StorageDBException.class, StorageDBExceptionTest::execute);
    }
}
package com.storage.engine.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordUtilityTest {
    @Test
    void testIndexToAddress() {
        // Test cases within the range of Integer.MAX_VALUE.
        assertEquals(10, RecordUtility.indexToAddress(10, 0));
        assertEquals(20, RecordUtility.indexToAddress(10, 1));
        assertEquals(1280, RecordUtility.indexToAddress(10, 127));
        assertEquals(1304, RecordUtility.indexToAddress(10, 128));

        // Test for record indices which cross the Integer.MAX_VALUE limit.
        long maxIndex = Integer.MAX_VALUE - 1;
        assertEquals(21709717480L, RecordUtility.indexToAddress(10, maxIndex));
    }

    @Test
    void testAddressToIndex() {
        // Test cases within the range of Integer.MAX_VALUE.
        assertEquals(0, RecordUtility.addressToIndex(10, 10));
        assertEquals(126, RecordUtility.addressToIndex(10, 1270));
        assertEquals(127, RecordUtility.addressToIndex(10, 1284));

        // Test for large addresses.
        long maxAddress = 21709717480L;
        assertEquals(Integer.MAX_VALUE - 1, RecordUtility.addressToIndex(10, maxAddress));
    }
}
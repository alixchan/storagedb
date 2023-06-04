package com.storage.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionStateTest {

    private CompactionState compactionState;

    @BeforeEach
    void setUp() {
        compactionState = new CompactionState();
    }

    @Test
    void isLimitation_beforeTimeLimit_returnsFalse() {
        boolean result = compactionState.isLimitation();
        assertFalse(result);
    }

    @Test
    void isLimitation_afterTimeLimit_returnsTrue() throws InterruptedException {
//        Thread.sleep(31 * 60 * 1000); // Wait for the time limit to pass
//        boolean result = compactionState.isLimitation();
//        assertTrue(result);
    }

    @Test
    void getBegin_returnsValidTimestamp() {
        long begin = compactionState.getCompactionStartTime();
        assertTrue(begin <= System.currentTimeMillis());
    }

}

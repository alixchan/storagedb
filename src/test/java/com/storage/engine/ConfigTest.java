package com.storage.engine;

import com.storage.engine.map.IndexMap;
import com.storage.engine.map.IndexMapImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigTest {

    private Config config;

    @BeforeEach
    void setUp() {
        config = new Config();
    }

    @Test
    void autoCompactEnabled_defaultValue_returnsTrue() {
        assertTrue(config.autoCompactEnabled());
    }

    @Test
    void isAutoCompact_defaultValue_returnsTrue() {
        assertTrue(config.isAutoCompact());
    }

    @Test
    void setValueSize_validValue_setsValueSize() {
        int valueSize = 256;
        config.setValueSize(valueSize);
        assertEquals(valueSize, config.getValueSize());
    }

    @Test
    void setDbDir_validValue_setsDbDir() {
        String dbDir = "/path/to/db";
        config.setDatabaseDirectory(dbDir);
        assertEquals(dbDir, config.getDatabaseDirectory());
    }

    @Test
    void setIndexMap_validValue_setsIndexMap() {
        IndexMap indexMap = new IndexMapImpl();
        config.setIndexMap(indexMap);
        assertEquals(indexMap, config.getIndexMap());
    }

    @Test
    void setCompactionWaitTimeoutMs_validValue_setsCompactionWaitTimeoutMs() {
        long timeoutMs = 5000;
        config.setCompactionWaitTimeoutMs(timeoutMs);
        assertEquals(timeoutMs, config.getCompactionWaitTimeoutMs());
    }

    @Test
    void setMinBuffersToCompact_validValue_setsMinBuffersToCompact() {
        int minBuffers = 6;
        config.setMinBuffersToCompact(minBuffers);
        assertEquals(minBuffers, config.getMinBuffersToCompact());
    }

    @Test
    void setDataToWalFileRatio_validValue_setsDataToWalFileRatio() {
        int dataToWalRatio = 5;
        config.setDataToWalFileRatio(dataToWalRatio);
        assertEquals(dataToWalRatio, config.getDataToWalFileRatio());
    }

    @Test
    void setBufferFlushTimeoutMs_validValue_setsBufferFlushTimeoutMs() {
        long timeoutMs = 3000;
        config.setBufferFlushTimeoutMs(timeoutMs);
        assertEquals(timeoutMs, config.getBufferFlushTimeoutMs());
    }

    @Test
    void setMaxBufferSize_validValue_setsMaxBufferSize() {
        int bufferSize = 10 * 1024 * 1024;
        config.setMaxBufferSize(bufferSize);
        assertEquals(bufferSize, config.getMaxBufferSize());
    }

    @Test
    void setOpenFDCount_validValue_setsOpenFDCount() {
        int openFDCount = 20;
        config.setOpenFileDescriptorCount(openFDCount);
        assertEquals(openFDCount, config.getOpenFileDescriptorCount());
    }

}


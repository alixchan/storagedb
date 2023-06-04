package com.storage.engine;

import com.storage.engine.exceptions.ConfigException;
import com.storage.engine.map.IndexMap;
import com.storage.engine.map.IndexMapImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class StorageDBBuilderTest {

    private StorageDBBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new StorageDBBuilder();
    }

    @Test
    void withAutoCompactDisabled_configUpdatedWithAutoCompactDisabled() {
        StorageDBBuilder result = builder.AutoCompactDisabled();
        assertFalse(result.conf.autoCompactEnabled());
    }

    @Test
    void withValueSize_configUpdatedWithValueSize() {
        int valueSize = 256;
        StorageDBBuilder result = builder.ValueSize(valueSize);
        assertEquals(valueSize, result.conf.getValueSize());
    }

    @Test
    void withDbDir_configUpdatedWithDbDir() {
        String dbDir = "/path/to/db";
        StorageDBBuilder result = builder.Dir(dbDir);
        assertEquals(dbDir, result.conf.getDatabaseDirectory());
    }

    @Test
    void withCompactionWaitTimeoutMs_configUpdatedWithCompactionWaitTimeoutMs() {
        long timeoutMs = 5000;
        StorageDBBuilder result = builder.CompactionWaitTimeoutMs(timeoutMs);
        assertEquals(timeoutMs, result.conf.getCompactionWaitTimeoutMs());
    }

    @Test
    void withMinBuffersToCompact_configUpdatedWithMinBuffersToCompact() {
        int minBuffers = 6;
        StorageDBBuilder result = builder.MinBuffersToCompact(minBuffers);
        assertEquals(minBuffers, result.conf.getMinBuffersToCompact());
    }

    @Test
    void withDataToWalFileRatio_configUpdatedWithDataToWalFileRatio() {
        int dataToWalRatio = 5;
        StorageDBBuilder result = builder.DataToWalFileRatio(dataToWalRatio);
        assertEquals(dataToWalRatio, result.conf.getDataToWalFileRatio());
    }

    @Test
    void withBufferFlushTimeoutMs_configUpdatedWithBufferFlushTimeoutMs() {
        long timeoutMs = 3000;
        StorageDBBuilder result = builder.BufferFlushTimeoutMs(timeoutMs);
        assertEquals(timeoutMs, result.conf.getBufferFlushTimeoutMs());
    }

    @Test
    void withMaxBufferSize_configUpdatedWithMaxBufferSize() {
        int bufferSize = 10 * 1024 * 1024;
        StorageDBBuilder result = builder.MaxBufferSize(bufferSize);
        assertEquals(bufferSize, result.conf.getMaxBufferSize());
    }

    @Test
    void withIndexMap_configUpdatedWithIndexMap() {
        IndexMap indexMap = new IndexMapImpl();
        StorageDBBuilder result = builder.IndexMap(indexMap);
        assertEquals(indexMap, result.conf.getIndexMap());
    }

    @Test
    void withDbDir_path_configUpdatedWithDbDir() {
        Path path = Path.of("/path/to/db");
        StorageDBBuilder result = builder.Dir(path);
        assertEquals(path.toString(), result.conf.getDatabaseDirectory());
    }

    @Test
    void withMaxOpenFDCount_configUpdatedWithMaxOpenFDCount() {
        int openFDCount = 20;
        StorageDBBuilder result = builder.MaxOpenFDCount(openFDCount);
        assertEquals(openFDCount, result.conf.getOpenFileDescriptorCount());
    }

    @Test
    void build_validConfig_returnsStorageDB() throws IOException {
        String dbDir = "/path/to/db";
        int valueSize = 256;
        builder.Dir(dbDir)
                .ValueSize(valueSize);
        StorageDB storageDB = builder.build();
        assertNotNull(storageDB);
    }

    @Test
    void build_nullDbDir_throwsConfigException() {
        builder.Dir((String) null);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_emptyDbDir_throwsConfigException() {
        builder.Dir("");
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_valueSizeZero_throwsConfigException() {
        builder.ValueSize(0);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_compactionWaitTimeoutLessThanMin_throwsConfigException() {
        builder.CompactionWaitTimeoutMs(Config.MIN_COMPACTION_WAIT_TIMEOUT_MS - 1);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_bufferFlushTimeoutNegative_throwsConfigException() {
        builder.BufferFlushTimeoutMs(-1000);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_minBuffersToCompactLessThanFloor_throwsConfigException() {
        builder.MinBuffersToCompact(Config.FLOOR_MIN_BUFFERS_TO_COMPACT - 1);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_dataToWalFileRatioLessThanMin_throwsConfigException() {
        builder.DataToWalFileRatio(Config.MIN_DATA_TO_WAL_FILE_RATIO - 1);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_dataToWalFileRatioGreaterThanMax_throwsConfigException() {
        builder.DataToWalFileRatio(Config.MAX_DATA_TO_WAL_FILE_RATIO + 1);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_openFDCountGreaterThanMax_throwsConfigException() {
        builder.MaxOpenFDCount(Config.MAX_OPEN_FD_COUNT + 1);
        assertThrows(ConfigException.class, builder::build);
    }

    @Test
    void build_openFDCountLessThanMin_throwsConfigException() {
        builder.MaxOpenFDCount(Config.MIN_OPEN_FD_COUNT - 1);
        assertThrows(ConfigException.class, builder::build);
    }
}

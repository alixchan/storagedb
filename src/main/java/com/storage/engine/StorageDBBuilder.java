package com.storage.engine;

import com.storage.engine.exceptions.ConfigException;
import com.storage.engine.map.IndexMap;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The builder class for creating a {@link StorageDB} instance with custom configuration.
 */
public class StorageDBBuilder {
    /**
     * The configuration object for the {@link StorageDB} instance.
     */
    final Config conf = new Config();

    /**
     * Disables automatic compaction in the {@link StorageDB} instance.
     *
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder AutoCompactDisabled() {
        conf.autoCompact = false;
        return this;
    }

    /**
     * Sets the value size for the {@link StorageDB} instance.
     *
     * @param valueSize The value size to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder ValueSize(int valueSize) {
        conf.valueSize = valueSize;
        return this;
    }

    /**
     * Sets the directory path for the {@link StorageDB} instance.
     *
     * @param dbDir The directory path to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder Dir(String dbDir) {
        conf.databaseDirectory = dbDir;
        return this;
    }

    /**
     * Sets the compaction wait timeout in milliseconds for the {@link StorageDB} instance.
     *
     * @param compactionWaitTimeoutMs The compaction wait timeout to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder CompactionWaitTimeoutMs(long compactionWaitTimeoutMs) {
        conf.compactionWaitTimeoutMs = compactionWaitTimeoutMs;
        return this;
    }

    /**
     * Sets the minimum number of buffers to compact in the {@link StorageDB} instance.
     *
     * @param minBuffersToCompact The minimum number of buffers to compact.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder MinBuffersToCompact(int minBuffersToCompact) {
        conf.minBuffersToCompact = minBuffersToCompact;
        return this;
    }

    /**
     * Sets the data to WAL (Write-Ahead Log) file ratio in the {@link StorageDB} instance.
     *
     * @param dataToWalFileRatio The data to WAL file ratio to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder DataToWalFileRatio(int dataToWalFileRatio) {
        conf.dataToWalFileRatio = dataToWalFileRatio;
        return this;
    }

    /**
     * Sets the buffer flush timeout in milliseconds for the {@link StorageDB} instance.
     *
     * @param bufferFlushTimeoutMs The buffer flush timeout to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder BufferFlushTimeoutMs(long bufferFlushTimeoutMs) {
        conf.bufferFlushTimeoutMs = bufferFlushTimeoutMs;
        return this;
    }

    /**
     * Sets the maximum buffer size for the {@link StorageDB} instance.
     *
     * @param maxBufferSize The maximum buffer size to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder MaxBufferSize(int maxBufferSize) {
        conf.maxBufferSize = maxBufferSize;
        return this;
    }

    /**
     * Sets the index map for the {@link StorageDB} instance.
     *
     * @param indexMap The index map to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder IndexMap(IndexMap indexMap) {
        conf.indexMap = indexMap;
        return this;
    }

    /**
     * Sets the directory path for the {@link StorageDB} instance using a Path object.
     *
     * @param path The Path object representing the directory path to set.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder Dir(Path path) {
        return Dir(path.toString());
    }

    /**
     * Sets the maximum count of open file descriptors for the StorageDB instance.
     *
     * @param openFDCount The maximum count of open file descriptors.
     * @return The StorageDBBuilder instance.
     */
    public StorageDBBuilder MaxOpenFDCount(int openFDCount) {
        conf.openFileDescriptorCount = openFDCount;
        return this;
    }

    /**
     * Builds and returns the StorageDB instance with the configured parameters.
     *
     * @return The built {@link StorageDB} instance.
     * @throws IOException if an I/O error occurs during the creation of the StorageDB instance.
     */
    public StorageDB build() throws IOException {
        if (conf.databaseDirectory == null || conf.databaseDirectory.isEmpty()) {
            throw new ConfigException("Directory path cannot be empty or null.");
        }
        if (conf.valueSize == 0) {
            throw new ConfigException("ValueSize cannot be 0.");
        }
        if (conf.compactionWaitTimeoutMs < Config.MIN_COMPACTION_WAIT_TIMEOUT_MS) {
            throw new ConfigException("Compaction timeout cannot be less than " +
                    Config.MIN_COMPACTION_WAIT_TIMEOUT_MS);
        }
        if (conf.bufferFlushTimeoutMs < 0) {
            throw new ConfigException("Buffer flush timeout cannot be less than 0");
        }

        if (conf.minBuffersToCompact < Config.FLOOR_MIN_BUFFERS_TO_COMPACT) {
            throw new ConfigException("Min buffers to compact cannot be less than " +
                    Config.FLOOR_MIN_BUFFERS_TO_COMPACT);
        }
        if (conf.dataToWalFileRatio < Config.MIN_DATA_TO_WAL_FILE_RATIO) {
            throw new ConfigException("Data to wal size ratio cannot be less than " +
                    Config.MIN_DATA_TO_WAL_FILE_RATIO);
        }
        if (conf.dataToWalFileRatio > Config.MAX_DATA_TO_WAL_FILE_RATIO) {
            throw new ConfigException("Data to wal size ratio cannot be greater than " +
                    Config.MAX_DATA_TO_WAL_FILE_RATIO);
        }
        if (conf.openFileDescriptorCount > Config.MAX_OPEN_FD_COUNT) {
            throw new ConfigException("Open FD count cannot be greater than " +
                    Config.MAX_OPEN_FD_COUNT);
        }
        if (conf.openFileDescriptorCount < Config.MIN_OPEN_FD_COUNT) {
            throw new ConfigException("Open FD count cannot be less than " +
                    Config.MIN_OPEN_FD_COUNT);
        }

        return new StorageDB(conf);
    }
}

package com.storage.engine;

import com.storage.engine.map.IndexMap;

/**
 * Configuration class (see {@link Config}) for managing database settings.
 */
public class Config {

    /**
     * The number of records per block.
     */
    public static final int RECORDS_PER_BLOCK = 128;

    /**
     * The size of the CRC (Cyclic Redundancy Check) value.
     */
    public static final int CRC_SIZE = 4;

    /**
     * The size of the key.
     */
    public static final int KEY_SIZE = 4;

    /**
     * The maximum size of a value.
     */
    static final int MAX_VALUE_SIZE = 512 * 1024;

    /**
     * The default compaction wait timeout in milliseconds.
     */
    private static final long DEFAULT_COMPACTION_WAIT_TIMEOUT_MS = (long) 1000 * 60;

    /**
     * The minimum compaction wait timeout in milliseconds.
     */
    static final long MIN_COMPACTION_WAIT_TIMEOUT_MS = (long) 1000 * 30;

    /**
     * The default minimum number of buffers to compact.
     */
    private static final int DEFAULT_MIN_BUFFERS_TO_COMPACT = 8;

    /**
     * The floor value for the minimum number of buffers to compact.
     */
    static final int FLOOR_MIN_BUFFERS_TO_COMPACT = 4;

    /**
     * The default ratio of data size to WAL (Write-Ahead Log) file size.
     */
    private static final int DEFAULT_DATA_TO_WAL_FILE_RATIO = 10;

    /**
     * The minimum ratio of data size to WAL file size.
     */
    static final int MIN_DATA_TO_WAL_FILE_RATIO = 2;

    /**
     * The maximum ratio of data size to WAL file size.
     */
    static final int MAX_DATA_TO_WAL_FILE_RATIO = 100;

    /**
     * The default buffer flush timeout in milliseconds.
     */
    private static final long DEFAULT_BUFFER_FLUSH_TIMEOUT_MS = (long) 1000 * 60;

    /**
     * The default maximum buffer size.
     */
    private static final int DEFAULT_MAX_BUFFER_SIZE = 4 * 1024 * 1024;

    /**
     * The default count of open file descriptors.
     */
    private static final int DEFAULT_OPEN_FD_COUNT = 10;

    /**
     * The minimum count of open file descriptors.
     */
    static final int MIN_OPEN_FD_COUNT = 1;

    /**
     * The maximum count of open file descriptors.
     */
    static final int MAX_OPEN_FD_COUNT = 100;

    /**
     * Specifies whether auto compaction is enabled.
     */
    boolean autoCompact = true;

    /**
     * The size of the value.
     */
    int valueSize;

    /**
     * The directory of the database.
     */
    String databaseDirectory;

    /**
     * The index map (see {@link IndexMap}).
     */
    IndexMap indexMap;

    /**
     * The compaction wait timeout in milliseconds.
     */
    long compactionWaitTimeoutMs = DEFAULT_COMPACTION_WAIT_TIMEOUT_MS;

    /**
     * The minimum number of buffers to compact.
     */
    int minBuffersToCompact = DEFAULT_MIN_BUFFERS_TO_COMPACT;

    /**
     * The ratio of data size to WAL file size.
     */
    int dataToWalFileRatio = DEFAULT_DATA_TO_WAL_FILE_RATIO;

    /**
     * The buffer flush timeout in milliseconds.
     */
    long bufferFlushTimeoutMs = DEFAULT_BUFFER_FLUSH_TIMEOUT_MS;

    /**
     * The maximum buffer size.
     */
    int maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;

    /**
     * The count of open file descriptors.
     */
    int openFileDescriptorCount = DEFAULT_OPEN_FD_COUNT;

    /**
     * Checks if auto compaction is enabled.
     *
     * @return {@code true} if auto compaction is enabled, {@code false} otherwise.
     */
    public boolean autoCompactEnabled() {
        return autoCompact;
    }

    /**
     * Gets the size of the value.
     *
     * @return The size of the value.
     */
    public int getValueSize() {
        return valueSize;
    }

    /**
     * Gets the directory of the database.
     *
     * @return The directory of the database.
     */
    public String getDatabaseDirectory() {
        return databaseDirectory;
    }

    /**
     * Gets the default compaction wait timeout in milliseconds.
     *
     * @return The default compaction wait timeout in milliseconds.
     */
    public static long getDefaultCompactionWaitTimeoutMs() {
        return DEFAULT_COMPACTION_WAIT_TIMEOUT_MS;
    }

    /**
     * Gets the compaction wait timeout in milliseconds.
     *
     * @return The compaction wait timeout in milliseconds.
     */
    public long getCompactionWaitTimeoutMs() {
        return compactionWaitTimeoutMs;
    }

    /**
     * Checks if auto compaction is enabled.
     *
     * @return {@code true} if auto compaction is enabled, {@code false} otherwise.
     */
    public boolean isAutoCompact() {
        return autoCompact;
    }

    /**
     * Sets the auto compaction flag.
     *
     * @param autoCompact The value indicating whether auto compaction should be enabled or disabled.
     */
    public void setAutoCompact(boolean autoCompact) {
        this.autoCompact = autoCompact;
    }

    /**
     * Sets the size of the value.
     *
     * @param valueSize The size of the value.
     */
    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    /**
     * Sets the directory of the database.
     *
     * @param databaseDirectory The directory of the database.
     */
    public void setDatabaseDirectory(String databaseDirectory) {
        this.databaseDirectory = databaseDirectory;
    }

    /**
     * Sets the index map.
     *
     * @param indexMap The index map.
     */
    public void setIndexMap(IndexMap indexMap) {
        this.indexMap = indexMap;
    }

    /**
     * Sets the compaction wait timeout in milliseconds.
     *
     * @param compactionWaitTimeoutMs The compaction wait timeout in milliseconds.
     */
    public void setCompactionWaitTimeoutMs(long compactionWaitTimeoutMs) {
        this.compactionWaitTimeoutMs = compactionWaitTimeoutMs;
    }

    /**
     * Sets the minimum number of buffers to compact.
     *
     * @param minBuffersToCompact The minimum number of buffers to compact.
     */
    public void setMinBuffersToCompact(int minBuffersToCompact) {
        this.minBuffersToCompact = minBuffersToCompact;
    }

    /**
     * Sets the ratio of data size to WAL file size.
     *
     * @param dataToWalFileRatio The ratio of data size to WAL file size.
     */
    public void setDataToWalFileRatio(int dataToWalFileRatio) {
        this.dataToWalFileRatio = dataToWalFileRatio;
    }

    /**
     * Sets the buffer flush timeout in milliseconds.
     *
     * @param bufferFlushTimeoutMs The buffer flush timeout in milliseconds.
     */
    public void setBufferFlushTimeoutMs(long bufferFlushTimeoutMs) {
        this.bufferFlushTimeoutMs = bufferFlushTimeoutMs;
    }

    /**
     * Sets the maximum buffer size.
     *
     * @param maxBufferSize The maximum buffer size.
     */
    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Sets the count of open file descriptors.
     *
     * @param openFileDescriptorCount The count of open file descriptors.
     */
    public void setOpenFileDescriptorCount(int openFileDescriptorCount) {
        this.openFileDescriptorCount = openFileDescriptorCount;
    }

    /**
     * Gets the minimum number of buffers to compact.
     *
     * @return The minimum number of buffers to compact.
     */
    public int getMinBuffersToCompact() {
        return minBuffersToCompact;
    }

    /**
     * Gets the ratio of data size to WAL file size.
     *
     * @return The ratio of data size to WAL file size.
     */
    public int getDataToWalFileRatio() {
        return dataToWalFileRatio;
    }

    /**
     * Gets the buffer flush timeout in milliseconds.
     *
     * @return The buffer flush timeout in milliseconds.
     */
    public long getBufferFlushTimeoutMs() {
        return bufferFlushTimeoutMs;
    }

    /**
     * Gets the maximum buffer size.
     *
     * @return The maximum buffer size.
     */
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * Gets the count of open file descriptors.
     *
     * @return The count of open file descriptors.
     */
    public int getOpenFileDescriptorCount() {
        return openFileDescriptorCount;
    }

    /**
     * Gets the index map.
     *
     * @return The {@link IndexMap}.
     */
    public IndexMap getIndexMap() {
        return indexMap;
    }
}

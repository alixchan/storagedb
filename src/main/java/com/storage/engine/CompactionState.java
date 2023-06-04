package com.storage.engine;

import java.io.File;
import java.util.BitSet;

/**
 * The {@link CompactionState} class represents the state of compaction in a storage engine.
 */
class CompactionState {
    /**
     * The maximum time limit for compaction in milliseconds.
     */
    public static final int MAX_COMPACTION_TIME_MS = 30 * 60000;

    /**
     * The timestamp when the compaction process started.
     */
    private final long compactionStartTime = System.currentTimeMillis();

    /**
     * The index of the next record in the file.
     */
    long nextRecordIndex;

    /**
     * The BitSet representing data in the next file.
     */
    BitSet dataInNextDataFile = new BitSet();

    /**
     * The BitSet representing data in the next Write-Ahead Log (WAL) file.
     */
    BitSet dataInNextWalLogFile = new BitSet();

    /**
     * The next Write-Ahead Log (WAL) file.
     */
    File nextWalLogFile;

    /**
     * The next data file.
     */
    File nextDataFile;

    /**
     * Retrieves the index of the next record in the file.
     *
     * @return the index of the next record in the file
     */
    public long getNextRecordIndex() {
        return nextRecordIndex;
    }

    /**
     * Sets the index of the next record in the file.
     *
     * @param nextRecordIndex the index of the next record in the file
     */
    public void setNextRecordIndex(long nextRecordIndex) {
        this.nextRecordIndex = nextRecordIndex;
    }

    /**
     * Retrieves the BitSet representing data in the next file.
     *
     * @return the BitSet representing data in the next file
     */
    public BitSet getDataInNextDataFile() {
        return dataInNextDataFile;
    }

    /**
     * Sets the BitSet representing data in the next file.
     *
     * @param dataInNextDataFile the BitSet representing data in the next file
     */
    public void setDataInNextDataFile(BitSet dataInNextDataFile) {
        this.dataInNextDataFile = dataInNextDataFile;
    }

    /**
     * Retrieves the BitSet representing data in the next Write-Ahead Log (WAL) file.
     *
     * @return the BitSet representing data in the next WAL file
     */
    public BitSet getDataInNextWalLogFile() {
        return dataInNextWalLogFile;
    }

    /**
     * Checks if the compaction process has reached the time limitation.
     *
     * @return true if the time limitation has been reached, false otherwise
     */
    boolean isLimitation() {
        return System.currentTimeMillis() - compactionStartTime > MAX_COMPACTION_TIME_MS;
    }

    /**
     * Sets the BitSet representing data in the next Write-Ahead Log (WAL) file.
     *
     * @param dataInNextWalLogFile the BitSet representing data in the next WAL file
     */
    public void setDataInNextWalLogFile(BitSet dataInNextWalLogFile) {
        this.dataInNextWalLogFile = dataInNextWalLogFile;
    }

    /**
     * Retrieves the next Write-Ahead Log (WAL) file.
     *
     * @return the next WAL file
     */
    public File getNextWalLogFile() {
        return nextWalLogFile;
    }

    /**
     * Retrieves the timestamp when the compaction process started.
     *
     * @return the timestamp when the compaction process started
     */
    public long getCompactionStartTime() {
        return compactionStartTime;
    }

    /**
     * Sets the next Write-Ahead Log (WAL) file.
     *
     * @param nextWalLogFile the next WAL file
     */
    public void setNextWalLogFile(File nextWalLogFile) {
        this.nextWalLogFile = nextWalLogFile;
    }

    /**
     * Retrieves the next data file.
     *
     * @return the next data file
     */
    public File getNextDataFile() {
        return nextDataFile;
    }

    /**
     * Sets the next data file.
     *
     * @param nextDataFile the next data file
     */
    public void setNextDataFile(File nextDataFile) {
        this.nextDataFile = nextDataFile;
    }
}

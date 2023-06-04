package com.storage.engine.utilities;

import static com.storage.engine.Config.CRC_SIZE;
import static com.storage.engine.Config.RECORDS_PER_BLOCK;

/**
 * Utility class for working with records (addressing computations) in a storage system.
 */
public class RecordUtility {
    /**
     * Private constructor to prevent instantiation of the utility class.
     */
    private RecordUtility() {
    }

    /**
     * Calculates the block size with trailer for a given record size.
     *
     * @param recordSize the size of each record
     * @return the block size with trailer
     */
    public static int totalBlockSize(int recordSize) {
        return recordSize * RECORDS_PER_BLOCK + CRC_SIZE + recordSize;
    }

    /**
     * Converts the record index to its corresponding address in the storage system.
     *
     * @param size  the size of each record
     * @param index the index of the record
     * @return the address corresponding to the record index
     */
    public static long indexToAddress(int size, long index) {
        long blockOffset = index / RECORDS_PER_BLOCK;
        long recordOffset = index % RECORDS_PER_BLOCK;
        return blockOffset * (long) totalBlockSize(size) + (recordOffset + 1)* size;
    }

    /**
     * Converts the address in the storage system to its corresponding record index.
     *
     * @param recordSize the size of each record
     * @param address    the address in the storage system (absolute record address)
     * @return the record index corresponding to the address
     */
    public static int addressToIndex(int recordSize, long address) {
        int blockSize = totalBlockSize(recordSize);
        address -= recordSize;
        return (int) ((address / blockSize) * RECORDS_PER_BLOCK + ((address % blockSize) / recordSize));
    }
}

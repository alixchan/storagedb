package com.storage.engine.map;

import com.storage.engine.StorageDB;

/**
 * The {@code IndexMap} interface defines the contract for a map-like data structure that
 * stores key/index mappings.
 */
public interface IndexMap {

    /**
     * API to support put for the key/index pair in question. Key {@link StorageDB#RESERVED_KEY_MARKER}
     * is reserved and custom implementations must make sure reserved keys are not used.
     *
     * @param key        The key to be inserted
     * @param indexValue The index mapping for the key
     */
    void put(int key, int indexValue);


    /**
     * API to support for get for the key. If get fails, return {@link StorageDB#RESERVED_KEY_MARKER}
     * which represents null or not found.
     *
     * @param key The key whose index value is to be retrieved.
     * @return The index value for the key asked.
     */
    int get(int key);


    /**
     * Get size of index.
     *
     * @return Size of the index.
     */
    int size();
}

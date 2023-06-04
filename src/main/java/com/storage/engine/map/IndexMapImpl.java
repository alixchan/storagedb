package com.storage.engine.map;

import com.storage.engine.StorageDB;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * An implementation of the {@link IndexMap} interface.
 */
public class IndexMapImpl implements IndexMap {

    /**
     * The underlying hash map that stores the index mappings.
     */
    private final TIntIntHashMap indexMap;

    /**
     * The initial capacity of the index map.
     */
    private static final int CAPACITY = 100_000;

    /**
     * The load factor of the index map.
     */
    private static final float LOAD_FACTOR = 0.95F;

    /**
     * Constructs an {@code IndexMapImpl} with the default capacity and load factor.
     * Uses the {@link StorageDB#RESERVED_KEY_MARKER} for default values.
     */
    public IndexMapImpl() {
        this(CAPACITY, LOAD_FACTOR);
    }

    /**
     * Constructs an {@code IndexMapImpl} with the specified initial capacity and load factor.
     * Uses the {@link StorageDB#RESERVED_KEY_MARKER} for default values.
     *
     * @param initialCapacity The initial capacity of the index map.
     * @param loadFactor      The load factor of the index map.
     */
    public IndexMapImpl(int initialCapacity, float loadFactor) {
        indexMap = new TIntIntHashMap(initialCapacity, loadFactor, StorageDB.RESERVED_KEY_MARKER,
                StorageDB.RESERVED_KEY_MARKER);
    }

    /**
     * Associates the specified index value with the specified key in this index map.
     *
     * @param key        The key with which the index value is to be associated.
     * @param indexValue The index value to be associated with the key.
     */
    @Override
    public void put(int key, int indexValue) {
        indexMap.put(key, indexValue);
    }

    /**
     * Returns the index value to which the specified key is mapped, or the default value if the key is not
     * present in the index map.
     *
     * @param key The key whose associated index value is to be returned.
     * @return The index value to which the specified key is mapped.
     */
    @Override
    public int get(int key) {
        return indexMap.get(key);
    }

    /**
     * Returns the number of key-value mappings in this index map.
     *
     * @return The number of key-value mappings in this index map.
     */
    @Override
    public int size() {
        return indexMap.size();
    }
}


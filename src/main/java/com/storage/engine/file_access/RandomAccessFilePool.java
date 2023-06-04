package com.storage.engine.file_access;

import com.storage.engine.exceptions.StorageDBRuntimeException;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import java.io.File;
/**
 * A pool implementation for managing instances of {@link RandomAccessFileWrapper}.
 * Extends {@link GenericKeyedObjectPool}.
 */
public class RandomAccessFilePool extends GenericKeyedObjectPool<File, RandomAccessFileWrapper> {
    /**
     * Constructs a new RandomAccessFilePool instance with the specified number of open file descriptors.
     *
     * @param openFds The maximum count of open file descriptors.
     */
    public RandomAccessFilePool(int openFds) {
        super(new RandomAccessFileFactory(), getConfig(openFds));
    }
    /**
     * Creates and returns a configuration for the RandomAccessFilePool.
     *
     * @param openFds The maximum count of open file descriptors.
     * @return The configuration for the RandomAccessFilePool.
     */
    private static GenericKeyedObjectPoolConfig<RandomAccessFileWrapper> getConfig(int openFds) {
        final GenericKeyedObjectPoolConfig<RandomAccessFileWrapper> config =
                new GenericKeyedObjectPoolConfig<>();
        config.setMaxTotalPerKey(openFds);
        config.setBlockWhenExhausted(true);
        config.setTestOnBorrow(true);
        config.setTestOnCreate(true);
        return config;
    }
    /**
     * Borrow an instance of RandomAccessFileWrapper associated with the specified key (file).
     *
     * @param key The key (file) associated with the RandomAccessFileWrapper.
     * @return The borrowed RandomAccessFileWrapper instance.
     * @throws StorageDBRuntimeException If an exception occurs while borrowing the object.
     */
    @Override
    public RandomAccessFileWrapper borrowObject(File key) {
        try {
            return super.borrowObject(key);
        } catch (Exception e) {
            throw new StorageDBRuntimeException(e);
        }
    }
}

package com.storage.engine.file_access;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.File;

/**
 * A factory for creating and managing instances of {@link RandomAccessFileWrapper}.
 * Implements {@link KeyedPooledObjectFactory}.
 */
public class RandomAccessFileFactory implements KeyedPooledObjectFactory<File, RandomAccessFileWrapper> {
    /**
     * Creates and returns a new PooledObject containing a RandomAccessFileWrapper for the specified file.
     *
     * @param file The file associated with the RandomAccessFileWrapper.
     * @return The created PooledObject containing the RandomAccessFileWrapper instance.
     * @throws Exception If an exception occurs while creating the object.
     */
    @Override
    public PooledObject<RandomAccessFileWrapper> makeObject(File file) throws Exception {
        return new DefaultPooledObject<>(new RandomAccessFileWrapper(file, "r"));
    }

    /**
     * Destroys the PooledObject by closing the associated RandomAccessFileWrapper.
     *
     * @param file         The file associated with the RandomAccessFileWrapper.
     * @param pooledObject The PooledObject to be destroyed.
     * @throws Exception If an exception occurs while destroying the object.
     */
    @Override
    public void destroyObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject)
            throws Exception {
        pooledObject.getObject().close();
    }

    /**
     * Validates the PooledObject by checking if the associated RandomAccessFileWrapper is associated with
     * the specified file.
     *
     * @param file         The file associated with the RandomAccessFileWrapper.
     * @param pooledObject The PooledObject to be validated.
     * @return {@code true} if the RandomAccessFileWrapper is associated with the specified file, {@code
     * false} otherwise.
     */
    @Override
    public boolean validateObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject) {
        return pooledObject.getObject().isSame(file);
    }

    /**
     * Activates the PooledObject.
     *
     * @param file         The file associated with the RandomAccessFileWrapper.
     * @param pooledObject The PooledObject to be activated.
     */
    @Override
    public void activateObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject) {
        // No action required.
    }

    /**
     * Passivates the PooledObject.
     *
     * @param file         The file associated with the RandomAccessFileWrapper.
     * @param pooledObject The PooledObject to be passivated.
     */
    @Override
    public void passivateObject(File file, PooledObject<RandomAccessFileWrapper> pooledObject) {
        // No action required.
    }
}

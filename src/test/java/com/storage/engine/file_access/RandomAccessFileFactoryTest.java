package com.storage.engine.file_access;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class RandomAccessFileFactoryTest {

    private KeyedPooledObjectFactory<File, RandomAccessFileWrapper> factory;

    @BeforeEach
    void setUp() {
        factory = new RandomAccessFileFactory();
    }

    @Test
    void makeObject_createsRandomAccessFileWrapper() throws Exception {
        File file = new File(".\\src\\test\\resources\\test.txt");
        PooledObject<RandomAccessFileWrapper> pooledObject = factory.makeObject(file);
        RandomAccessFileWrapper wrapper = pooledObject.getObject();
        assertNotNull(wrapper);
        assertTrue(wrapper instanceof RandomAccessFileWrapper);
        assertEquals(file, wrapper.getFile());
    }

    @Test
    void destroyObject_closesRandomAccessFileWrapper() throws Exception {
        File file = new File(".\\src\\test\\resources\\test.txt");
        PooledObject<RandomAccessFileWrapper> pooledObject = factory.makeObject(file);
        RandomAccessFileWrapper wrapper = pooledObject.getObject();
        factory.destroyObject(file, pooledObject);
        assertThrows(IOException.class, wrapper::getFilePointer);
    }

    @Test
    void validateObject_validatesRandomAccessFileWrapper() throws Exception {
        File file = new File(".\\src\\test\\resources\\test.txt");
        PooledObject<RandomAccessFileWrapper> pooledObject = factory.makeObject(file);
        boolean result = factory.validateObject(file, pooledObject);
        assertTrue(result);
    }

    @Test
    void validateObject_invalidatesRandomAccessFileWrapper() throws Exception {
        File file1 = new File(".\\src\\test\\resources\\file1.txt");
        File file2 = new File(".\\src\\test\\resources\\file2.txt");
        PooledObject<RandomAccessFileWrapper> pooledObject = factory.makeObject(file1);
        boolean result = factory.validateObject(file2, pooledObject);
        assertFalse(result);
    }

    @Test
    void activateObject_noActionRequired() {
        File file = new File(".\\src\\test\\resources\\test.txt");
        PooledObject<RandomAccessFileWrapper> pooledObject = null;
        try {
            pooledObject = factory.makeObject(file);
            factory.activateObject(file, pooledObject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // No assertions, just verifying that no exception is thrown
    }

    @Test
    void passivateObject_noActionRequired(){
        File file = new File(".\\src\\test\\resources\\file1.txt");
        PooledObject<RandomAccessFileWrapper> pooledObject = null;
        try {
            pooledObject = factory.makeObject(file);
            factory.passivateObject(file, pooledObject);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // No assertions, just verifying that no exception is thrown
    }
}

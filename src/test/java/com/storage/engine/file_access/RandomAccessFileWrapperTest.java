package com.storage.engine.file_access;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

class RandomAccessFileWrapperTest {
    @Test
    void isSameFile_sameFile_returnsTrue() throws FileNotFoundException {
        File file = new File("test.txt");
        RandomAccessFileWrapper wrapper = new RandomAccessFileWrapper(file, "rw");
        boolean result = wrapper.isSame(file);
        assertTrue(result);
    }

    @Test
    void isSameFile_differentFile_returnsFalse() throws FileNotFoundException {
        File file1 = new File("file1.txt");
        File file2 = new File("file2.txt");
        RandomAccessFileWrapper wrapper = new RandomAccessFileWrapper(file1, "rw");
        boolean result = wrapper.isSame(file2);
        assertFalse(result);
    }

    @Test
    void getFile_returnsCorrectFile() throws FileNotFoundException {
        File file = new File("test.txt");
        RandomAccessFileWrapper wrapper = new RandomAccessFileWrapper(file, "rw");
        File result = wrapper.getFile();
        assertEquals(file, result);
    }
}


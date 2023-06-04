package com.storage.engine.file_access;

import com.storage.engine.exceptions.StorageDBRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class RandomAccessFilePoolTest {

    private RandomAccessFilePool filePool;
    private File testFile;

    @BeforeEach
    void setUp() throws IOException {
        testFile = File.createTempFile("test", ".txt");
        filePool = new RandomAccessFilePool(5);
    }

    @Test
    void borrowObject_existingFile_returnsValidObject() throws Exception {
        RandomAccessFileWrapper fileWrapper;
        fileWrapper = filePool.borrowObject(testFile);
        assertNotNull(fileWrapper);
        assertTrue(fileWrapper.getFile().exists());
        assertTrue(fileWrapper.getFile().canRead());
        assertTrue(fileWrapper.getFile().canWrite());
    }

    @Test
    void borrowObject_multipleBorrow_returnsDistinctObjects() throws Exception {
        RandomAccessFileWrapper fileWrapper1;
        RandomAccessFileWrapper fileWrapper2;
        fileWrapper1 = filePool.borrowObject(testFile);
        fileWrapper2 = filePool.borrowObject(testFile);
        assertNotSame(fileWrapper1, fileWrapper2);
    }

    @Test
    void borrowObject_invalidFile_throwsRuntimeException() {
        File invalidFile = new File("nonexistent.txt");
        assertThrows(StorageDBRuntimeException.class, () -> filePool.borrowObject(invalidFile));
    }

    @Test
    void borrowObject_closedFilePool_throwsRuntimeException() throws Exception {
        filePool.close();
        assertThrows(StorageDBRuntimeException.class, () -> filePool.borrowObject(testFile));
    }
}

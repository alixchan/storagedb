package com.storage.engine.file_access;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * A wrapper class that extends {@link RandomAccessFile} and provides additional functionality.
 */
public class RandomAccessFileWrapper extends RandomAccessFile {
    /**
     * File.
     */
    private final File file;

    /**
     * Constructs a new RandomAccessFileWrapper instance given a file and mode.
     *
     * @param f    The file to be opened for random access.
     * @param mode The access mode with which to open the file.
     * @throws FileNotFoundException If the file is not found.
     */
    public RandomAccessFileWrapper(File f, String mode) throws FileNotFoundException {
        super(f, mode);
        file = f;
    }

    /**
     * Checks if the specified file is the same as the one associated with this RandomAccessFileWrapper.
     *
     * @param f The file to compare.
     * @return {@code true} if the specified file is the same as the one associated with this
     * RandomAccessFileWrapper,
     * {@code false} otherwise.
     */
    public boolean isSame(File f) {
        return file == f;
    }

    /**
     * Returns the file associated with this RandomAccessFileWrapper.
     *
     * @return The file associated with this RandomAccessFileWrapper.
     */
    public File getFile() {
        return file;
    }
}

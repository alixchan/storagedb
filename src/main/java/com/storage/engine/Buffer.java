package com.storage.engine;

import com.storage.engine.exceptions.BufferException;
import com.storage.engine.exceptions.ExcessValueSizeException;
import com.storage.engine.exceptions.StorageDBRuntimeException;
import com.storage.engine.utilities.RecordUtility;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import static com.storage.engine.StorageDB.RESERVED_KEY_MARKER;

/**
 * The {@link Buffer} class represents a write buffer for the Write-Ahead Logging (WAL) file.
 * It provides methods to initialize, manage, and interact with the buffer.
 * <p>
 * If the index for a random retrieval points to an offset beyond the WAL file's actual location,
 * it is assumed to be located in the write buffer.
 */
public class Buffer {
    /**
     * The underlying {@link ByteBuffer} used to hold the buffer data.
     */
    private ByteBuffer buffer;
    /**
     * The size of each value stored in the buffer.
     */
    private final int valueSize;
    /**
     * The size of each record in the buffer, including the value and metadata.
     */
    private final int recordSize;
    /**
     * Indicates whether the buffer is read-only.
     * If true, modifications to the buffer's contents are not allowed.
     */
    private final boolean readOnly;
    /**
     * The configuration settings for the buffer and the WAL file (see {@link Config}).
     */
    private final Config config;
    /**
     * The maximum number of records that the buffer can hold.
     */
    private final int maximumRecords;

    /**
     * Initializes a write buffer for the Write-Ahead Logging (WAL) file with the specified configuration.
     * The buffer is initialized based on the provided database configuration and read-only flag.
     * <p>
     * The initialization process follows these steps:
     * Calculates the maximum number of records that can fit within a 4 MB buffer.
     * If the calculated value is less than the predefined constant {@link Config#RECORDS_PER_BLOCK},
     * it sets the maximum number of records to 128. This condition occurs for very large values.
     * Adjusts the maximum number of records to be a multiple of 128.
     * Calculates the number of bytes required to accommodate the CRCs and sync markers.
     * Finally, initializes a write buffer with the calculated number of bytes.
     *
     * @param config The configuration used to produce the database instance.
     * @param readOnly Indicates whether the buffer is read-only.
     */
    public Buffer(Config config, boolean readOnly) {
        this.valueSize = config.getValueSize();
        this.recordSize = valueSize + Config.KEY_SIZE;
        this.readOnly = readOnly;
        this.config = config;
        if (valueSize > Config.MAX_VALUE_SIZE) {
            throw new ExcessValueSizeException();
        }
        this.maximumRecords = calculateMaxRecords();
        buffer =
                ByteBuffer.allocate(this.maximumRecords / Config.RECORDS_PER_BLOCK * (Config.RECORDS_PER_BLOCK * recordSize + (Config.CRC_SIZE + recordSize)));
    }

    /**
     * Retrieves the capacity of the buffer.
     *
     * @return The capacity of the buffer.
     */
    int capacity() {
        return buffer.capacity();
    }

    /**
     * Calculates the maximum number of records that can be buffered based on the configuration.
     *
     * @return The maximum number of records to buffer.
     */
    int calculateMaxRecords() {
        return (Math.max(config.getMaxBufferSize() / recordSize,
                Config.RECORDS_PER_BLOCK) / Config.RECORDS_PER_BLOCK) * Config.RECORDS_PER_BLOCK;
    }

    /**
     * Retrieves the maximum number of records that can be stored in the buffer.
     *
     * @return The maximum number of records.
     */
    public int getMaximumRecords() {
        return maximumRecords;
    }

    /**
     * Retrieves the write buffer size.
     *
     * @return The size of the write buffer.
     */
    int getWriteBufferSize() {
        return buffer.capacity();
    }

    /**
     * Flushes the contents of the buffer to the specified output stream.
     *
     * @param out The output stream to flush the buffer to.
     * @return The number of bytes flushed.
     * @throws IOException if an I/O error occurs.
     */
    public int flush(OutputStream out) throws IOException {
        assert !readOnly : "Read-only.";
        if (buffer.position() == 0) {
            return 0;
        }
        while ((RecordUtility.addressToIndex(recordSize, buffer.position())) % Config.RECORDS_PER_BLOCK != 0) {
            add(buffer.getInt(buffer.position() - recordSize), buffer.array(),
                    buffer.position() - recordSize + Config.KEY_SIZE);
        }
        int bytes = buffer.position();
        out.write(buffer.array(), 0, bytes);
        out.flush();
        return bytes;
    }

    /**
     * Reads data from multiple files and applies a record consumer to each record.
     *
     * @param files          The list of files to read from.
     * @param reverse        Indicates whether to read the files in reverse order.
     * @param recordConsumer The consumer function to apply to each record.
     * @throws IOException If an I/O error occurs.
     */
    void readFromFiles(List<RandomAccessFile> files, boolean reverse,
                       Consumer<ByteBuffer> recordConsumer) throws IOException {
        for (RandomAccessFile file : files) {
            readFromFile(file, reverse, recordConsumer);
        }
    }

    /**
     * Reads data from a file and applies a record consumer to each record.
     *
     * @param file           The file to read from.
     * @param reverse        Indicates whether to read the file in reverse order.
     * @param recordConsumer The consumer function to apply to each record.
     * @throws IOException If an I/O error occurs.
     */
    void readFromFile(RandomAccessFile file, boolean reverse, Consumer<ByteBuffer> recordConsumer) throws IOException {
        int blockSize = RecordUtility.totalBlockSize(recordSize);

        if (reverse && file.getFilePointer() % blockSize != 0) {
            throw new StorageDBRuntimeException("Inconsistent data for iteration.");
        }

        long filePointer = file.getFilePointer();
        long validBytesRemaining = reverse ? filePointer - buffer.capacity() : 0;

        while (filePointer > 0) {
            buffer.clear();
            validBytesRemaining = Math.max(validBytesRemaining, 0);
            file.seek(validBytesRemaining);
            fillBuffer(file, recordConsumer, true);
            filePointer = validBytesRemaining;
        }

        if (!reverse) {
            while (fillBuffer(file, recordConsumer, false) >= blockSize) {
                buffer.clear();
            }
        }
    }


    /**
     * Reads data from a random access file and fills a buffer with the read bytes, then applies a consumer
     * function
     * to each record in the buffer.
     *
     * @param file           The random access file to read from.
     * @param recordConsumer The consumer function to apply to each record in the buffer.
     * @param reverse        Indicates whether to iterate over the records in reverse order.
     * @return The number of bytes read from the file.
     * @throws IOException If an I/O error occurs.
     */
    private int fillBuffer(RandomAccessFile file, Consumer<ByteBuffer> recordConsumer, boolean reverse) throws IOException {
        int bytesRead = file.read(buffer.array());
        if (bytesRead == -1) {
            return 0;
        }
        buffer.position(bytesRead).limit(bytesRead);
        iterator(reverse).asIterator().forEachRemaining(recordConsumer);
        return bytesRead;
    }


    /**
     * Retrieves the byte array backing the buffer.
     *
     * @return The byte array backing the buffer.
     */
    byte[] array() {
        return buffer.array();
    }

    /**
     * Checks if the buffer is dirty (contains data).
     *
     * @return {@code true} if the buffer is dirty, {@code false} otherwise.
     */
    boolean isDirty() {
        return buffer.position() > 0;
    }

    /**
     * Checks if the buffer is full.
     *
     * @return {@code true} if the buffer is full, {@code false} otherwise.
     */
    boolean isFull() {
        return buffer.remaining() == 0;
    }

    /**
     * Adds a key-value pair to the buffer at the current position.
     *
     * @param key         The key to be added
     * @param value       The byte array containing the value
     * @param valueOffset The offset in the value byte array
     * @return The address in the buffer where the key-value pair is added
     * @throws BufferException If the buffer is in read-only mode
     */
    public int add(int key, byte[] value, int valueOffset) {
        if (readOnly) {
            throw new BufferException("Read only.");
        }

        if (buffer.position() % RecordUtility.totalBlockSize(recordSize) == 0) {
            insertSyncMarker();
        }

        int position = buffer.position();
        buffer.putInt(key);
        buffer.put(value, valueOffset, valueSize);
        if (RecordUtility.addressToIndex(recordSize, buffer.position()) % Config.RECORDS_PER_BLOCK == 0) {
            closeBlock();
        }
        return position;
    }

    /**
     * Attempts to update a key in the in-memory buffer after verifying the key.
     *
     * @param key             The key to be updated
     * @param newValue        The byte array containing the new value
     * @param valueOffset     The offset in the value byte array
     * @param addressInBuffer The address in the buffer at which the key-value pair exists
     * @return {@code true} if the update succeeds after key verification, {@code false} otherwise
     */
    boolean update(int key, byte[] newValue, int valueOffset, int addressInBuffer) {
        if (buffer.getInt(addressInBuffer) == key) {
            System.arraycopy(newValue, valueOffset, buffer.array(), addressInBuffer + Config.KEY_SIZE,
                    valueSize);
            return true;
        }
        return false;
    }


    /**
     * Returns an enumeration of byte buffers representing the key-value pairs in the buffer.
     *
     * @param reverse {@code true} if the enumeration should be in reverse order, {@code false} otherwise
     * @return An enumeration of byte buffers
     */
    Enumeration<ByteBuffer> iterator(boolean reverse) {
        ByteBuffer ourBuffer = buffer.duplicate();

        int recordsToRead = 0;
        if (buffer.position() > 0) {
            recordsToRead = RecordUtility.addressToIndex(recordSize, buffer.position());
        }

        int initialRecordIndex = reverse ? recordsToRead : 0;
        int finalRecordsToRead = recordsToRead;

        return new Enumeration<>() {
            int currentRecordIndex = initialRecordIndex;

            @Override
            public boolean hasMoreElements() {
                return reverse ? currentRecordIndex != 0 : currentRecordIndex < finalRecordsToRead;
            }

            @Override
            public ByteBuffer nextElement() {
                int position = (int) RecordUtility.indexToAddress(recordSize, reverse ?
                        --currentRecordIndex : currentRecordIndex++);
                ourBuffer.position(position);
                return ourBuffer;
            }
        };
    }

    /**
     * Closes the current block by calculating and updating the CRC32 value in the buffer.
     */
    private void closeBlock() {
        CRC32 crc = new CRC32();
        int blockSize = recordSize * Config.RECORDS_PER_BLOCK;
        crc.update(buffer.array(), buffer.position() - blockSize, blockSize);
        buffer.putInt((int) crc.getValue());
    }

    /**
     * Generates a synchronization marker byte array used for block operations.
     *
     * @param valueSize The size of the value in the marker.
     * @return The synchronization marker byte array.
     */
    public static byte[] getSyncMarker(int valueSize) {
        byte[] syncMarker = new byte[valueSize + Config.KEY_SIZE];
        Arrays.fill(syncMarker, (byte) 0xFF);
        ByteBuffer.wrap(syncMarker).putInt(0, RESERVED_KEY_MARKER);
        return syncMarker;
    }

    /**
     * Gets the sync marker byte array of the specified size.
     */
    protected void insertSyncMarker() {
        buffer.put(getSyncMarker(valueSize));
    }

    /**
     * Clears the buffer by allocating a new byte buffer with the same capacity.
     */
    public void clear() {
        buffer = ByteBuffer.allocate(buffer.capacity());
    }
}

package com.storage.engine;

import java.io.IOException;

/**
 * The {@link RecordConsumer} interface provides a method for accepting records.
 */
public interface RecordConsumer {
    /**
     * Accepts a record with the given key and data at the specified offset.
     *
     * @param key    the key of the record
     * @param bytes   the data of the record
     * @param offset the offset of the record in the data array
     * @throws IOException if an I/O error occurs while accepting the record
     */
    void accept(int key, byte[] bytes, int offset) throws IOException;
}

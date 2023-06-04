package com.storage.engine.utilities;

import java.util.Deque;

/**
 * A utility class for working with byte arrays and conversions.
 */
public class ByteUtility {
    /**
     * Private constructor to prevent instantiation of the class.
     */
    private ByteUtility() {
    }

    /**
     * Converts a portion of a byte array to an integer.
     *
     * @param bytes  the byte array
     * @param offset the starting offset in the byte array
     * @return the integer value (representation of byte sequence)
     */
    public static int toInt(byte[] bytes, int offset) {
        return bytes[offset] << 24 | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8) | bytes[offset + 3] & 0xFF;
    }

    /**
     * Compares the given byte array with a deque of bytes for equality.
     *
     * @param arrayBytes The byte array to compare
     * @param dequeBytes The deque of bytes to compare
     * @return {@code true} if the byte array and deque contain the same elements in the same order, {@code
     * false} otherwise
     */
    public static boolean arraysEqual(byte[] arrayBytes, Deque<Byte> dequeBytes) {
        if (dequeBytes == null || arrayBytes == null || dequeBytes.size() != arrayBytes.length) {
            return false;
        }
        int idx = 0;
        for (Byte aByte : dequeBytes)
            if (aByte != arrayBytes[idx++]) {
                return false;
            }
        return true;
    }
}

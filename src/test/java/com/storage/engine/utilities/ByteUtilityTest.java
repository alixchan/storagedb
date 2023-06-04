package com.storage.engine.utilities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ByteUtilityTest {

    @ParameterizedTest
    @ValueSource(ints = {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1, 28, Integer.MAX_VALUE / 2})
    void toIntShouldReturnSameValue(final int val) {
        final ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(2, val);
        assertEquals(val, ByteUtility.toInt(buf.array(), 2));
    }

    @Test
    void testArrayEquals() {
        final ArrayDeque<Byte> deque = new ArrayDeque<>();
        final byte[] bytes = new byte[100];

        assertFalse(ByteUtility.arraysEqual(bytes, null));
        assertFalse(ByteUtility.arraysEqual(null, deque));
        assertFalse(ByteUtility.arraysEqual(null, null));
        assertFalse(ByteUtility.arraysEqual(bytes, deque));

        fillDequeWithZeros(deque, bytes.length);
        assertTrue(ByteUtility.arraysEqual(bytes, deque));

        fillDequeWithRandomBytes(deque, bytes);
        assertFalse(ByteUtility.arraysEqual(bytes, deque));

        fillDequeFromArray(deque, bytes);
        assertTrue(ByteUtility.arraysEqual(bytes, deque));

        deque.removeFirst();
        assertFalse(ByteUtility.arraysEqual(bytes, deque));
    }

    private void fillDequeWithZeros(ArrayDeque<Byte> deque, int length) {
        IntStream.range(0, length)
                .mapToObj(i -> (byte) 0)
                .forEach(deque::add);
    }


    private void fillDequeWithRandomBytes(ArrayDeque<Byte> deque, byte[] bytes) {
        ThreadLocalRandom.current().nextBytes(bytes);
        for (byte aByte : bytes) deque.add(aByte);
    }

    private void fillDequeFromArray(ArrayDeque<Byte> deque, byte[] bytes) {
        deque.clear();
        for (byte aByte : bytes) deque.add(aByte);
    }

    @Test
    public void testToInt() {
        byte[] data = {0x00, 0x00, 0x00, 0x01};
        int result = ByteUtility.toInt(data, 0);
        assertEquals(1, result);
    }

    @Test
    public void testArrayEquals_LengthMismatch() {
        byte[] primitiveBytes = {0x00, 0x01, 0x02};
        Deque<Byte> bytes = new ArrayDeque<>(Arrays.asList((byte) 0x00, (byte) 0x01));
        boolean result = ByteUtility.arraysEqual(primitiveBytes, bytes);
        assertFalse(result);
    }

    @Test
    public void testArrayEquals_Match() {
        byte[] primitiveBytes = {0x00, 0x01, 0x02};
        Deque<Byte> bytes = new ArrayDeque<>(Arrays.asList((byte) 0x00, (byte) 0x01, (byte) 0x02));
        boolean result = ByteUtility.arraysEqual(primitiveBytes, bytes);
        assertTrue(result);
    }

    @Test
    public void testArrayEquals_NoMatch() {
        byte[] primitiveBytes = {0x00, 0x01, 0x02};
        Deque<Byte> bytes = new ArrayDeque<>(Arrays.asList((byte) 0x00, (byte) 0x01, (byte) 0x03));
        boolean result = ByteUtility.arraysEqual(primitiveBytes, bytes);
        assertFalse(result);
    }

}
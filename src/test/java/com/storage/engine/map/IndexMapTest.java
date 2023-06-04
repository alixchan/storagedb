package com.storage.engine.map;

import com.storage.engine.StorageDB;
import com.storage.engine.StorageDBBuilder;
import com.storage.engine.exceptions.StorageDBException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Map implements IndexMap {
    private final HashMap<Integer, Integer> map = new HashMap<>();
    private int putCount;
    private int getCount;

    @Override
    public void put(int key, int indexValue) {
        putCount++;
        map.put(key, indexValue);
    }

    @Override
    public int get(int key) {
        getCount++;
        if (!map.containsKey(key)) {
            return StorageDB.RESERVED_KEY_MARKER;
        }
        return map.get(key);
    }

    @Override
    public int size() {
        return map.size();
    }

    public int getPutCount() {
        return putCount;
    }

    public int getGetCount() {
        return getCount;
    }
}

class IndexMapTest {
    @Test
    void testCustomMap() throws IOException, StorageDBException {
        final int vSize = 10;
        Map indexMap = new Map();
        final StorageDB db = new StorageDBBuilder()
                .Dir(Files.createTempDirectory("storage").toString())
                .ValueSize(vSize)
                .IndexMap(indexMap)
                .build();

        assertEquals(0, db.size());

        final ByteBuffer v = ByteBuffer.allocate(vSize);
        v.putLong(100L);
        db.put(0, v.array());
        assertEquals(1, indexMap.getPutCount());

        assertAll(() -> {
                    assertEquals(100, ByteBuffer.wrap(db.randomGet(0)).getLong());
                },
                () -> {
                    assertEquals(1, indexMap.getPutCount());
                },
                () -> {
                    assertEquals(1, db.size());
                }
        );
    }
}

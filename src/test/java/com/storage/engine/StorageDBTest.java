package com.storage.engine;

import com.storage.engine.exceptions.ConfigException;
import com.storage.engine.exceptions.RetainedKeyException;
import com.storage.engine.exceptions.StorageDBException;
import com.storage.engine.exceptions.StorageDBRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class StorageDBTest {

    @Test
    void simpleTest() throws IOException, StorageDBException, InterruptedException {
        final Path path = Files.createTempDirectory("storagedb");

        final int valueSize = 28;
        final StorageDB db = new StorageDBBuilder()
                .Dir(path.toString())
                .ValueSize(valueSize)
                .AutoCompactDisabled()
                .build();

        assertEquals(0, db.size());

        final int records = 100;
        for (int i = 0; i < records; i++) {
            assertNull(db.randomGet(i));
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putInt((int) (Math.random() * 100000000)); // Insert a random value.
            db.put(i, value.array());

            value.clear();
            value.putInt(i); // Insert a predictable value.
            db.put(i, value.array());
        }

        assertEquals(records, db.size());

        // Verify.
        for (int i = 0; i < records; i++) {
            final byte[] bytes = db.randomGet(i);
            final ByteBuffer value = ByteBuffer.wrap(bytes);
            assertEquals(i, value.getInt());
        }

        // Iterate sequentially.
        db.iterate((key, data, offset) -> {
            final ByteBuffer value = ByteBuffer.wrap(data, offset, valueSize);
            assertEquals(key, value.getInt());
        });

        db.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {0/*, 349_440*/})
    void compactionTest(final int totalRecords)
            throws IOException, StorageDBException, InterruptedException {
        final Path path = Files.createTempDirectory("storagedb");

        final int valueSize = 8;
        final StorageDB db = new StorageDBBuilder()
                .Dir(path.toString())
                .ValueSize(valueSize)
                .AutoCompactDisabled()
                .build();

        final HashMap<Integer, Long> kvCache = new HashMap<>();

        for (int i = 0; i < totalRecords; i++) {
            long val = (long) (Math.random() * Long.MAX_VALUE);
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putLong(val); // Insert a random value.
            db.put(i, value.array());
            kvCache.put(i, val);
        }

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        // Now compact
        db.compact();

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        int count = totalRecords / 2;
        while (count-- > 0) {
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            long val = (long) (Math.random() * Long.MAX_VALUE);
            value.putLong(val); // Insert a random value.
            final int randomKey = (int) (Math.random() * totalRecords);
            db.put(randomKey, value.array());
            kvCache.put(randomKey, val);
        }

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        db.compact();

        // Make sure all is well
        verifyDb(db, totalRecords, kvCache);

        db.close();
    }

    private void verifyDb(StorageDB db, int records, HashMap<Integer, Long> kvCache)
            throws IOException, StorageDBException {
        // Verify.
        for (int i = 0; i < records; i++) {
            final byte[] bytes = db.randomGet(i);
            final ByteBuffer value = ByteBuffer.wrap(bytes);
            assertEquals(kvCache.get(i), value.getLong());
        }
    }

    @Test
    void Test() throws IOException, StorageDBException, InterruptedException {
        final Path path = Files.createTempDirectory("storagedb");

        final int valueSize = 28;
        final StorageDB db = new StorageDBBuilder()
                .Dir(path.toString())
                .ValueSize(valueSize)
                .AutoCompactDisabled()
                .build();

        assertEquals(0, db.size());
        db.close();
    }


    @Test
    void verifyPersistenceOfValueSize() throws IOException, InterruptedException {
        final String dbDir = Files.createTempDirectory("storage").toString();
        final StorageDB db = new StorageDBBuilder()
                .Dir(dbDir)
                .ValueSize(8)
                .build();

        db.close();

        final StorageDBBuilder builder = new StorageDBBuilder()
                .Dir(dbDir)
                .ValueSize(16);
        assertThrows(ConfigException.class, builder::build);
    }

    private boolean isCompactionComplete(Config conf) throws InterruptedException {
        File dataFile = new File(conf.getDatabaseDirectory() + File.separator + "data");
        File walFile = new File(conf.getDatabaseDirectory() + File.separator + "wal");
        File nextDataFile = new File(conf.getDatabaseDirectory() + File.separator + "data.next");
        File nextWalFile = new File(conf.getDatabaseDirectory() + File.separator + "wal.next");
        if (nextDataFile.exists()) {
            return false;
        }
        if (nextWalFile.exists()) {
            return false;
        }
        if (walFile.length() != 0) {
            return false;
        }
        if (dataFile.length() == 0) {
            return false;
        }
        return true;
    }

    @Test
    void testMultipleConfigurations() throws IOException {
        final Path path = Files.createTempDirectory("storage");
        StorageDB db = new StorageDBBuilder()
                .ValueSize(100)
                .Dir(path)
                .AutoCompactDisabled()
                .CompactionWaitTimeoutMs(45 * 1000)
                .BufferFlushTimeoutMs(30 * 1000)
                .DataToWalFileRatio(25)
                .MaxBufferSize(8 * 1024 * 1024)
                .MinBuffersToCompact(5)
                .MaxOpenFDCount(40)
                .build();

        final Config dbConfig = db.getConfig();
        assertEquals(100, dbConfig.getValueSize());
        assertEquals(path.toString(), dbConfig.getDatabaseDirectory());
        assertFalse(dbConfig.autoCompactEnabled());
        assertEquals(30 * 1000, dbConfig.getBufferFlushTimeoutMs());
        assertEquals(45 * 1000, dbConfig.getCompactionWaitTimeoutMs());
        assertEquals(25, dbConfig.getDataToWalFileRatio());
        assertEquals(8 * 1024 * 1024, dbConfig.getMaxBufferSize());
        assertEquals(5, dbConfig.getMinBuffersToCompact());
        assertEquals(40, dbConfig.getOpenFileDescriptorCount());
    }

    @Test
    void flushSimulateInfiniteCompaction() throws IOException {
        final StorageDB db = buildDB(Files.createTempDirectory("storage"), 10);

        final CompactionState state = Mockito.mock(CompactionState.class);
        when(state.getCompactionStartTime()).thenReturn(System.currentTimeMillis() - 31 * 60 * 1000L);
        when(state.isLimitation()).thenReturn(true);

        db.put(1, new byte[10]);
        
        db.compactionState = state;

        assertNull(db.exceptionDuringBackgroundOps);
        db.flush();

        assertNotNull(db.exceptionDuringBackgroundOps);
    }

    @Test
    void verifyPutFailure() throws IOException {
        final StorageDB db = buildDB(Files.createTempDirectory("storage"), 100);
        db.put(1, new byte[100]);

        db.exceptionDuringBackgroundOps = new StorageDBRuntimeException();
        assertThrows(StorageDBRuntimeException.class, () -> db.put(1, new byte[100]));

        db.exceptionDuringBackgroundOps = null;
        db.put(1, new byte[100]);
    }

    private StorageDB buildDB(Path dbPath, int valueSize) throws IOException {
        return new StorageDBBuilder()
                .ValueSize(valueSize)
                .Dir(dbPath)
                .build();
    }

    @Test
    void testIncorrectConfiguration() throws IOException {
        StorageDBBuilder builder = new StorageDBBuilder();
        assertThrows(ConfigException.class, builder::build);

        final Path path = Files.createTempDirectory("storage");
        builder = new StorageDBBuilder().Dir(path);
        assertThrows(ConfigException.class, builder::build);

        builder = new StorageDBBuilder().Dir(path).ValueSize(10)
                .BufferFlushTimeoutMs(-1);
        assertThrows(ConfigException.class, builder::build);

        builder = new StorageDBBuilder().Dir(path).ValueSize(10)
                .CompactionWaitTimeoutMs(100);
        assertThrows(ConfigException.class, builder::build);

        for (int invalidBuffers : new int[]{0, 2}) {
            builder = new StorageDBBuilder().Dir(path).ValueSize(10)
                    .MinBuffersToCompact(invalidBuffers);
            assertThrows(ConfigException.class, builder::build);
        }

        for (int invalidRatio : new int[]{0, 101}) {
            builder = new StorageDBBuilder().Dir(path).ValueSize(10)
                    .DataToWalFileRatio(invalidRatio);
            assertThrows(ConfigException.class, builder::build);
        }

        for (int invalidOpenFds : new int[]{0, 101}) {
            builder = new StorageDBBuilder().Dir(path).ValueSize(10)
                    .MaxOpenFDCount(invalidOpenFds);
            assertThrows(ConfigException.class, builder::build);
        }
    }

    @Test
    void put() throws IOException, StorageDBException {
        final StorageDB db = new StorageDBBuilder()
                .Dir(Files.createTempDirectory("storage"))
                .ValueSize(8)
                .build();

        assertThrows(RetainedKeyException.class,
                () -> db.put(StorageDB.RESERVED_KEY_MARKER, new byte[1]));

        final byte[] value = new byte[8];
        ThreadLocalRandom.current().nextBytes(value);

        db.put(1, value);
        assertArrayEquals(value, db.randomGet(1));

        final byte[] largeByteArr = new byte[value.length + 100];
        System.arraycopy(value, 0, largeByteArr, 50, value.length);
        db.put(2, largeByteArr, 50);
        assertArrayEquals(value, db.randomGet(2));

        final byte[] key = new byte[4];
        final ByteBuffer keyBuf = ByteBuffer.wrap(key);
        keyBuf.putInt(Integer.MAX_VALUE - 200);
        db.put(key, value);
        assertArrayEquals(value, db.randomGet(Integer.MAX_VALUE - 200));

        keyBuf.clear();
        keyBuf.putInt(Integer.MAX_VALUE - 100);
        db.put(key, largeByteArr, 50);
        assertArrayEquals(value, db.randomGet(Integer.MAX_VALUE - 100));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 100, 1000, 10_000, 100_000, 1_000_000})
    void testBuildIndex(final int totalRecords)
            throws IOException, InterruptedException, StorageDBException {
        final Path path = Files.createTempDirectory("storage");
        System.out.println(path.toString() + " for " + totalRecords);
        final int valueSize = 8;

        StorageDB db = new StorageDBBuilder()
                .Dir(path.toString())
                .ValueSize(valueSize)
                .build();
        final HashMap<Integer, Long> kvCache = new HashMap<>();
        for (int i = 0; i < totalRecords; i++) {
            long val = i * 2;
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putLong(val); // Insert a random value.
            db.put(i, value.array());
            kvCache.put(i, val);
        }
        db.close();

        db = new StorageDBBuilder()
                .Dir(path.toString())
                .ValueSize(valueSize)
                .AutoCompactDisabled()
                .build();
        // Verify here.
        verifyDb(db, totalRecords, kvCache);
        db.close();
    }

    @Test
    void testInMemoryUpdate() throws IOException, StorageDBException {
        final Path path = Files.createTempDirectory("storage");

        final int valueSize = 28;
        final StorageDB db = new StorageDBBuilder()
            .Dir(path.toString())
            .ValueSize(valueSize)
            .AutoCompactDisabled()
            .build();

        assertEquals(0, db.size());

        int[] keysToInsert = {1, 2, 3, 1, 2};
        int[] valuesToInsert = {10, 11, 12, 13, 14};
        HashMap<Integer, Integer> keyValue = new HashMap<>();
        for (int i = 0; i < keysToInsert.length; i++) {
            final ByteBuffer value = ByteBuffer.allocate(valueSize);
            value.putInt(valuesToInsert[i]);
            db.put(keysToInsert[i], value.array());
            value.clear();
            keyValue.put(keysToInsert[i], valuesToInsert[i]);
        }
        byte[] storedValueBytes = db.randomGet(1);
        ByteBuffer storedValue = ByteBuffer.wrap(storedValueBytes);
        assertEquals(13, storedValue.getInt());

        storedValueBytes = db.randomGet(2);
        storedValue = ByteBuffer.wrap(storedValueBytes);
        assertEquals(14, storedValue.getInt());

        db.iterate((key, data, offset) -> {
            final ByteBuffer value = ByteBuffer.wrap(data, offset, valueSize);
            assertEquals(keyValue.get(key), value.getInt());
        });
        assertEquals(3, db.size());
    }
}
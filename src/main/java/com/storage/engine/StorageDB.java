package com.storage.engine;

import com.storage.engine.exceptions.*;
import com.storage.engine.file_access.RandomAccessFilePool;
import com.storage.engine.file_access.RandomAccessFileWrapper;
import com.storage.engine.map.IndexMap;
import com.storage.engine.map.IndexMapImpl;
import com.storage.engine.utilities.BlockUtility;
import com.storage.engine.utilities.ByteUtility;
import com.storage.engine.utilities.RecordUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * The {@link StorageDB} class represents a key-value storage database (that stores data in a database file).
 * It provides methods for storing, retrieving, and managing data in the database.
 * <p>
 * Protocol: key (4 bytes) | value (fixed bytes).
 */
public class StorageDB {
    /**
     * Marker for reserved keys.
     */
    public static final int RESERVED_KEY_MARKER = 0xffffffff;
    /**
     * The data-file name.
     */
    private static final String DATA_FILENAME = "data";
    /**
     * The WAL-file name.
     */
    private static final String WAL_FILENAME = "wal";
    /**
     * The pointer-name for next file.
     */
    private static final String FILETYPE = ".next";
    public static final String META = "/meta";

    /**
     * The index map that stores key-value pairs.
     * Key: The actual key within this KV store.
     * Value: The offset (either in the data file, or in the WAL file).
     */
    private final IndexMap index;
    /**
     * A BitSet indicating which data entries are stored in the WAL file.
     */
    private BitSet dataInWalFile = new BitSet();
    /**
     * ReentrantReadWriteLock used for synchronization of read and write operations.
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * Pool of random access files used for reading and writing data.
     */
    private final RandomAccessFilePool filePool;
    /**
     * Buffer used for caching data before flushing to disk.
     */
    private final Buffer buffer;
    /**
     * The timestamp of the last buffer flush operation in milliseconds.
     */
    private long lastBufferFlushTimeMs;
    /**
     * The size of each record in the database.
     */
    private final int recordSize;
    /**
     * The total number of bytes stored in the WAL file.
     */
    private long bytesInWalFile = -1;
    /**
     * The directory where the database files are stored.
     */
    private final File dirFile;
    /**
     * The configuration settings for the database.
     */
    private final Config config;
    /**
     * The state of compaction.
     */
    CompactionState compactionState;
    /**
     * The data file used for storing key-value data.
     */
    private File dataFile;
    /**
     * The WAL file used for write-ahead logging.
     */
    private File walFile;
    /**
     * The output stream for writing data to the WAL file.
     */
    private DataOutputStream walOut;
    /**
     * The worker thread responsible for background operations.
     */
    private Thread tWorker;
    /**
     * Synchronization object used for coordinating compaction operations.
     */
    private final Object compactionSync;
    /**
     * Lock object used for synchronizing compaction operations.
     */
    private final Object compactionLock = new Object();
    /**
     * Flag indicating whether the database is shut down.
     */
    private boolean shutDown = false;
    /**
     * Flag indicating whether the executor service is used for background operations.
     */
    private boolean useExecutorService = false;
    /**
     * Throwable object representing an exception occurred during background operations.
     */
    Throwable exceptionDuringBackgroundOps = null;
    /**
     * The logger for logging messages related to the StorageDB class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(StorageDB.class);
    /**
     * The executor service for running background tasks.
     */
    private static ExecutorService executorService;
    /**
     * The list of StorageDB instances served.
     */
    private static final ArrayList<StorageDB> instancesServed = new ArrayList<>();
    /**
     * The common synchronization object used for coordinating compaction operations across multiple
     * instances.
     */
    private static final Object commonCompactionSync = new Object();
    /**
     * Flag indicating whether the executor service is shut down.
     */
    private static boolean esShutDown = false;

    /**
     * Constructs a StorageDB object with the given configuration.
     *
     * @param config the configuration for the database
     * @throws IOException if an I/O error occurs
     */
    StorageDB(Config config) throws IOException {
        this.config = config;
        dirFile = new File(this.config.getDatabaseDirectory());
        dirFile.mkdirs();
        index = this.config.getIndexMap() == null ? new IndexMapImpl() : this.config.getIndexMap();
        recordSize = this.config.getValueSize() + Config.KEY_SIZE;
        buffer = new Buffer(this.config, false);
        lastBufferFlushTimeMs = System.currentTimeMillis();
        filePool = new RandomAccessFilePool(this.config.getOpenFileDescriptorCount());
        dataFile = new File(dirFile, DATA_FILENAME);
        walFile = new File(dirFile, WAL_FILENAME);
        File metaFile = new File(dirFile, META);

        if (metaFile.exists()) {
            byte[] bytes = Files.readAllBytes(metaFile.toPath());
            ByteBuffer meta = ByteBuffer.wrap(bytes);
            int valueSizeFromMeta = meta.getInt();
            if (valueSizeFromMeta != this.config.getValueSize()) {
                throw new ConfigException(this.config.getDatabaseDirectory()
                        + " contains database with the valuesize = "
                        + valueSizeFromMeta + " bytes, but " + this.config.getValueSize() + " bytes was " +
                        "provided.");
            }
        } else {
            ByteBuffer out = ByteBuffer.allocate(4);
            out.putInt(this.config.getValueSize());
            Files.write(metaFile.toPath(), out.array());
        }

        initWalOut();
        recover();
        buildIndex();

        if (executorService == null) {
            compactionSync = new Object();
            tWorker = new Thread(() -> {
                while (!shutDown) {
                    try {
                        synchronized (compactionSync) {
                            compactionSync.wait(this.config.getCompactionWaitTimeoutMs());
                        }
                        if (this.config.autoCompactEnabled() && shouldCompact()) {
                            LOG.info("AutoCompacting now.");
                            compact();
                        } else if (shouldFlushBuffer()) {
                            LOG.info("Flushing buffer to disk => timeout.");
                            flush();
                        }
                    } catch (Throwable e) {
                        LOG.error("Compaction failure.", e);
                        exceptionDuringBackgroundOps = e;
                    }
                }
            });
            tWorker.start();
        } else {
            useExecutorService = true;
            synchronized (instancesServed) {
                instancesServed.add(this);
            }
            compactionSync = commonCompactionSync;
        }
    }

    /**
     * Initializes the executor service with the specified number of threads.
     *
     * @param nThreads the number of threads
     */
    public static void initExecutorService(int nThreads) {
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads + 1);
        executorService.submit(() -> {
            boolean esShutDown = false;

            while (!esShutDown) {
                synchronized (commonCompactionSync) {
                    try {
                        commonCompactionSync.wait(Config.getDefaultCompactionWaitTimeoutMs());
                    } catch (InterruptedException e) {
                        handleInterruptedException(e);
                        esShutDown = true;
                    }
                }

                synchronized (instancesServed) {
                    for (StorageDB storageDB : instancesServed) {
                        try {
                            if (storageDB.config.autoCompactEnabled() && storageDB.shouldCompact()) {
                                LOG.info("Auto Compacting now.");
                                executorService.submit(() -> {
                                    try {
                                        storageDB.compact();
                                    } catch (IOException e) {
                                        handleCompactException(storageDB, e);
                                    }
                                });
                            } else if (storageDB.shouldFlushBuffer()) {
                                LOG.info("Flushing buffer to disk on timeout.");
                                storageDB.flush();
                            }
                        } catch (IOException e) {
                            handleFlushException(storageDB, e);
                        }
                    }
                }
            }
        });
    }

    private static void handleInterruptedException(InterruptedException e) {
        LOG.error("Interrupted while waiting for the common compaction sync lock", e);

        synchronized (instancesServed) {
            for (StorageDB storageDB : instancesServed) {
                storageDB.exceptionDuringBackgroundOps = e;
            }
        }

        executorService.shutdown();
        Thread.currentThread().interrupt();
    }

    private static void handleCompactException(StorageDB storageDB, IOException e) {
        storageDB.exceptionDuringBackgroundOps = e;
        LOG.error("Failed to compact!", e);
    }

    private static void handleFlushException(StorageDB storageDB, IOException e) {
        storageDB.exceptionDuringBackgroundOps = e;
        LOG.error("Failed to flush an open buffer!", e);
    }

    /**
     * Initializes the write-ahead log output stream.
     *
     * @throws FileNotFoundException if the file is not found
     */
    private void initWalOut() throws FileNotFoundException {
        walOut = new DataOutputStream(new FileOutputStream(walFile, true));
        bytesInWalFile = walFile.length();
    }

    /**
     * Checks if the buffer should be flushed based on the configured timeout.
     *
     * @return true if the buffer should be flushed, false otherwise
     */
    private boolean shouldFlushBuffer() {
        return System.currentTimeMillis() - lastBufferFlushTimeMs > config.getBufferFlushTimeoutMs();
    }

    /**
     * Checks if the database should be compacted based on the configured conditions.
     *
     * @return true if the database should be compacted, false otherwise
     */
    private boolean shouldCompact() {
        lock.readLock().lock();
        try {
            return isWalFileBigEnough(
                    isCompactionInProgress() ? compactionState.nextWalLogFile : walFile);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if the write-ahead log (WAL) file is big enough for compaction.
     *
     * @param walFile the WAL file to check
     * @return true if the WAL file is big enough for compaction, false otherwise
     */
    private boolean isWalFileBigEnough(File walFile) {
        return walFile.exists() && walFile.length() >=
                config.getMinBuffersToCompact() * buffer.capacity() &&
                (!dataFile.exists() || walFile.length() * config.getDataToWalFileRatio() >= dataFile.length());
    }

    /**
     * Builds the index for the data and WAL files.
     *
     * @throws IOException if an I/O error occurs
     */
    private void buildIndex() throws IOException {
        lock.readLock().lock();
        try {
            LOG.info("Build index for the DATA-file.");
            buildIndexFromFile(false);
            LOG.info("Build index for the WAL-file.");
            buildIndexFromFile(true);
            LOG.info("Finish building INDEX.");
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Builds the index from a file.
     *
     * @param isWal Determines whether the WAL (Write-Ahead Log) file should be used.
     * @throws IOException if an I/O error occurs.
     */
    private void buildIndexFromFile(boolean isWal) throws IOException {
        File file = isWal ? walFile : dataFile;
        if (file.exists()) {
            RandomAccessFileWrapper wrapper = filePool.borrowObject(file);
            int[] ints = {0};
            try {
                new Buffer(config, true).readFromFile(wrapper, false, entry -> {
                    int key = entry.getInt();
                    index.put(key, ints[0]++);
                    if (isWal) {
                        dataInWalFile.set(key);
                    }
                });
            } finally {
                filePool.returnObject(file, wrapper);
            }
        }
    }

    /**
     * Recovers the database if it's corrupted.
     * <p>
     * Calling this method brings the database to a state where exactly two files exist: the WAL file and
     * the data file.
     *
     * @throws IOException if an I/O error occurs.
     */
    private void recover() throws IOException {
        File nextWalFile = new File(dirFile, WAL_FILENAME + FILETYPE);
        File nextDataFile = new File(dirFile, DATA_FILENAME + FILETYPE);
        boolean nextWalFileDeleted = false;

        if (nextWalFile.exists()) {
            Files.copy(nextWalFile.toPath(), walOut);
            walOut.flush();
            initWalOut();
            Files.delete(nextWalFile.toPath());
            nextWalFileDeleted = true;
        }

        if (nextDataFile.exists() && !nextWalFileDeleted) {
            Files.copy(nextDataFile.toPath(), walOut);
            walOut.flush();
            Files.delete(nextDataFile.toPath());
        }

        walFile = BlockUtility.verifyBlocks(walFile, config.getValueSize());
        initWalOut();
        dataFile = BlockUtility.verifyBlocks(dataFile, config.getValueSize());
    }


    /**
     * Function moves a file atomically.
     *
     * @param file        The file to be moved.
     * @param destination The destination file.
     * @return A reference to the destination path.
     * @throws IOException if an I/O error occurs.
     */
    private Path move(File file, File destination) throws IOException {
        return Files.move(file.toPath(), destination.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Checks if compaction is in progress. This should always be called from synchronized context.
     *
     * @return true if compaction is in progress, false otherwise
     */
    private boolean isCompactionInProgress() {
        return compactionState != null;
    }

    /**
     * Performs compaction of the database.
     *
     * @throws IOException if an I/O error occurs during compaction
     */
    public void compact() throws IOException {
        synchronized (compactionLock) {
            long start = System.currentTimeMillis();
            lock.writeLock().lock();
            try {
                flush();
                LOG.info("Beginning compaction with bytesInWalFile={}", bytesInWalFile);
                if (bytesInWalFile == 0) {
                    return;
                }
                compactionState = new CompactionState();
                compactionState.nextWalLogFile = new File(dirFile.getAbsolutePath()
                        + File.separator + WAL_FILENAME + FILETYPE);
                walOut = new DataOutputStream(new FileOutputStream(compactionState.nextWalLogFile));
                bytesInWalFile = 0;
                compactionState.nextRecordIndex = 0;
            } finally {
                lock.writeLock().unlock();
            }
            compactionState.nextDataFile = new File(dirFile.getAbsolutePath() + File.separator +
                    DATA_FILENAME + FILETYPE);
            try (BufferedOutputStream out =
                         new BufferedOutputStream(new FileOutputStream(compactionState.nextDataFile),
                                 buffer.getWriteBufferSize())) {
                Buffer tmpBuffer = new Buffer(config, false);
                iterate(false, false, (key, data, offset) -> {
                    tmpBuffer.add(key, data, offset);
                    if (tmpBuffer.isFull()) {
                        flushNext(out, tmpBuffer);
                    }
                });
                if (tmpBuffer.isDirty()) {
                    flushNext(out, tmpBuffer);
                }
            }
            lock.writeLock().lock();
            try {
                walFile = move(compactionState.nextWalLogFile, walFile).toFile();
                dataFile = move(compactionState.nextDataFile, dataFile).toFile();
                dataInWalFile = compactionState.dataInNextWalLogFile;
                compactionState = null;
                filePool.clear();
            } finally {
                lock.writeLock().unlock();
            }
            LOG.info("Compaction completed successfully in {} ms",
                    System.currentTimeMillis() - start);
        }
    }

    /**
     * Flushes the buffer to the output stream.
     *
     * @param out    the output stream
     * @param buffer the buffer to flush
     * @throws IOException if an I/O error occurs during flushing
     */
    private void flushNext(OutputStream out, Buffer buffer) throws IOException {
        buffer.flush(out);
        try {
            lock.writeLock().lock();
            Enumeration<ByteBuffer> iterator = buffer.iterator(false);
            while (iterator.hasMoreElements()) {
                compactionState.nextRecordIndex++;
                int key = iterator.nextElement().getInt();
                if (!compactionState.dataInNextWalLogFile.get(key)) {
                    index.put(key, RecordUtility.addressToIndex(recordSize, RecordUtility
                            .indexToAddress(recordSize, compactionState.nextRecordIndex)));
                    compactionState.dataInNextDataFile.set(key);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        buffer.clear();
    }

    /**
     * Stores a key-value pair in the database.
     *
     * @param key         the key as a byte array
     * @param value       the value as a byte array
     * @param valueOffset the offset in the value array
     * @throws IOException if an I/O error occurs during the operation
     */
    public void put(byte[] key, byte[] value, int valueOffset)
            throws IOException {
        put(ByteUtility.toInt(key, 0), value, valueOffset);
    }

    /**
     * Stores a key-value pair in the database.
     *
     * @param key   the key as a byte array
     * @param value the value as a byte array
     * @throws IOException if an I/O error occurs during the operation
     */
    public void put(byte[] key, byte[] value) throws IOException {
        put(key, value, 0);
    }

    /**
     * Stores a key-value pair in the database.
     *
     * @param key   the key as an integer
     * @param value the value as a byte array
     * @throws IOException if an I/O error occurs during the operation
     */
    public void put(int key, byte[] value) throws IOException {
        put(key, value, 0);
    }

    /**
     * Stores a key-value pair in the database.
     *
     * @param key         the key as an integer
     * @param value       the value as a byte array
     * @param valueOffset the offset in the value array
     * @throws IOException if an I/O error occurs during the operation
     */
    public void put(int key, byte[] value, int valueOffset) throws IOException {
        if (exceptionDuringBackgroundOps != null) {
            throw new StorageDBRuntimeException("Will not accept writes.", exceptionDuringBackgroundOps);
        }
        if (key == RESERVED_KEY_MARKER) {
            throw new RetainedKeyException(RESERVED_KEY_MARKER);
        }
        lock.writeLock().lock();
        try {
            boolean updInPlace = false;
            int recordIndexForKey = index.get(key);
            if ((recordIndexForKey != RESERVED_KEY_MARKER)
                    && ((isCompactionInProgress() && compactionState.dataInNextWalLogFile.get(key))
                    || (!isCompactionInProgress() && dataInWalFile.get(key)))) {
                long address = RecordUtility.indexToAddress(recordSize, recordIndexForKey);
                if (address >= bytesInWalFile) {
                    updInPlace = buffer.update(key, value, valueOffset, (int) (address - bytesInWalFile));
                }
            }
            if (buffer.isFull()) {
                flush();
                synchronized (compactionSync) {
                    compactionSync.notifyAll();
                }
            }
            if (!updInPlace) {
                index.put(key, RecordUtility.addressToIndex(recordSize,
                        bytesInWalFile + buffer.add(key, value, valueOffset)));
            }
            if (isCompactionInProgress()) {
                compactionState.dataInNextWalLogFile.set(key);
            } else {
                dataInWalFile.set(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Flushes the write buffer to the WAL file.
     *
     * @throws IOException if an I/O error occurs during flushing
     */
    public void flush() throws IOException {
        if (!lock.writeLock().tryLock()) {
            return;
        }
        try {
            if (walOut == null || !buffer.isDirty()) {
                return;
            }
            bytesInWalFile += buffer.flush(walOut);
            buffer.clear();
            lastBufferFlushTimeMs = System.currentTimeMillis();
            if (isCompactionInProgress() && compactionState.isLimitation()) {
                long secondsSinceStart =
                        (System.currentTimeMillis() - compactionState.getCompactionStartTime()) / 1000;
                exceptionDuringBackgroundOps = new StorageDBRuntimeException("The last compaction has been " +
                        "running for over " + secondsSinceStart + " seconds!");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * Iterates over the records in the database.
     *
     * @param consumer the record consumer to handle each record
     * @throws IOException if an I/O error occurs during iteration
     */
    public void iterate(RecordConsumer consumer) throws IOException {
        iterate(true, true, consumer);
    }

    /**
     * Iterates over the records in the database and performs the specified actions.
     *
     * @param useLatestWalFile   Indicates whether to use the latest WAL (Write-Ahead Log) file during
     *                           iteration.
     * @param readInMemoryBuffer Indicates whether to read records from the in-memory buffer during iteration.
     * @param consumer           The consumer function to apply to each record.
     * @throws IOException in case of I/O error during the iteration.
     */
    private void iterate(boolean useLatestWalFile, boolean readInMemoryBuffer,
                         RecordConsumer consumer) throws IOException {
        ArrayList<RandomAccessFile> walFiles = new ArrayList<>(2);
        ArrayList<RandomAccessFile> dataFiles = new ArrayList<>(1);

        Enumeration<ByteBuffer> inMemRecords = null;
        lock.readLock().lock();
        try {
            if (isCompactionInProgress() && useLatestWalFile) {
                RandomAccessFileWrapper reader = filePool.borrowObject(compactionState.nextWalLogFile);
                reader.seek(reader.length());
                walFiles.add(reader);
            }

            if (walFile.exists()) {
                RandomAccessFileWrapper reader = filePool.borrowObject(walFile);
                reader.seek(reader.length());
                walFiles.add(reader);
            }

            if (dataFile.exists()) {
                RandomAccessFileWrapper reader = filePool.borrowObject(dataFile);
                reader.seek(0);
                dataFiles.add(reader);
            }

            if (readInMemoryBuffer) {
                inMemRecords = buffer.iterator(true);
            }
        } finally {
            lock.readLock().unlock();
        }

        BitSet set = new BitSet(index.size());

        Consumer<ByteBuffer> entryConsumer = entry -> {
            int key = entry.getInt();
            boolean read = set.get(key);
            if (!read) {
                try {
                    consumer.accept(key, entry.array(), entry.position());
                } catch (IOException e) {
                    throw new StorageDBRuntimeException(e);
                }
                set.set(key);
            }
        };

        if (readInMemoryBuffer) {
            while (inMemRecords.hasMoreElements()) {
                entryConsumer.accept(inMemRecords.nextElement());
            }
        }

        Consumer<List<RandomAccessFile>> returnFiles = files -> {
            for (RandomAccessFile file : files) {
                assert file instanceof RandomAccessFileWrapper;
                filePool.returnObject(((RandomAccessFileWrapper) file).getFile(),
                        (RandomAccessFileWrapper) file);
            }
        };

        Buffer reader = new Buffer(config, true);
        boolean returnDataFilesEarly = true;
        try {
            reader.readFromFiles(walFiles, true, entryConsumer);
            returnDataFilesEarly = false;
        } finally {
            returnFiles.accept(walFiles);
            if (returnDataFilesEarly) {
                returnFiles.accept(dataFiles);
            }
        }

        try {
            reader.readFromFiles(dataFiles, false, entryConsumer);
        } finally {
            returnFiles.accept(dataFiles);
        }
    }

    /**
     * Retrieves the value associated with the specified key from the database.
     *
     * @param key The key for which to retrieve the value.
     * @return The value associated with the key, or null if the key is not found.
     * @throws IOException        in case of I/O error during retrieval.
     * @throws StorageDBException if a storage-related error occurs during retrieval.
     */
    public byte[] randomGet(int key) throws IOException, StorageDBException {
        int recordIndex;
        RandomAccessFileWrapper f;
        byte[] value;
        lock.readLock().lock();
        long address;
        try {
            recordIndex = index.get(key);
            if (recordIndex == RESERVED_KEY_MARKER) {
                return null;
            }

            value = new byte[config.getValueSize()];

            if (isCompactionInProgress() && compactionState.dataInNextWalLogFile.get(key)) {
                address = RecordUtility.indexToAddress(recordSize, recordIndex);
                if (address >= bytesInWalFile) {
                    System.arraycopy(buffer.array(),
                            (int) (address - bytesInWalFile + Config.KEY_SIZE),
                            value, 0, config.getValueSize());
                    return value;
                }
                f = filePool.borrowObject(compactionState.nextWalLogFile);
            } else if (isCompactionInProgress() && compactionState.dataInNextDataFile.get(key)) {
                address = RecordUtility.indexToAddress(recordSize, recordIndex);
                f = filePool.borrowObject(compactionState.nextDataFile);
            } else if (dataInWalFile.get(key)) {
                address = RecordUtility.indexToAddress(recordSize, recordIndex);
                if (!isCompactionInProgress() && address >= bytesInWalFile) {
                    System.arraycopy(buffer.array(),
                            (int) (address - bytesInWalFile + Config.KEY_SIZE),
                            value, 0, config.getValueSize());
                    return value;
                }
                f = filePool.borrowObject(walFile);
            } else {
                address = RecordUtility.indexToAddress(recordSize, recordIndex);
                f = filePool.borrowObject(dataFile);
            }
        } finally {
            lock.readLock().unlock();
        }

        try {
            f.seek(address);
            if (f.readInt() != key) {
                throw new IncoherentDataException();
            }
            int bytesRead = f.read(value);
            if (bytesRead != config.getValueSize()) {
                throw new StorageDBException("Possible data corruption detected! "
                        + "Re-open the database for automatic recovery!");
            }
            return value;
        } finally {
            filePool.returnObject(f.getFile(), f);
        }
    }

    /**
     * Closes the database and releases any resources associated with it.
     *
     * @throws IOException          in case of I/O error during closing.
     * @throws InterruptedException if the current thread is interrupted while waiting for the operation to
     *                              complete.
     */
    public void close() throws IOException, InterruptedException {
        flush();
        shutDown = true;

        if (useExecutorService) {
            instancesServed.remove(this);
        } else {
            synchronized (compactionSync) {
                compactionSync.notifyAll();
            }
            tWorker.join();
        }
    }


    /**
     * Shuts down the executor service used for parallel operations.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting for the operation to
     *                              complete.
     */
    public static void shutDownExecutorService() throws InterruptedException {
        if (executorService != null) {
            esShutDown = true;
            synchronized (commonCompactionSync) {
                commonCompactionSync.notifyAll();
            }
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
                LOG.error("Unable to shutdown executor service in 5 minutes.");
                executorService.shutdownNow();
            }
            executorService = null;
        }
    }

    /**
     * Retrieves the configuration object used by the database.
     *
     * @return The configuration object.
     */
    public Config getConfig() {
        return config;
    }

    /**
     * Checks if the database is using an executor service for parallel operations.
     *
     * @return {@code true} if the database is using an executor service, {@code false} otherwise.
     */
    public boolean isUsingExecutorService() {
        return useExecutorService;
    }

    /**
     * Returns the number of records in the database.
     *
     * @return The number of records.
     */
    public int size() {
        return index.size();
    }
}

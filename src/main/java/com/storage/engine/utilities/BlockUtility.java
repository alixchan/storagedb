package com.storage.engine.utilities;

import com.storage.engine.Buffer;
import com.storage.engine.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.zip.CRC32;

/**
 * Utility class for block operations on files (recover blocks from a corrupted file).
 */
public class BlockUtility {
    /**
     * Private constructor to prevent instantiation of the class.
     */
    private BlockUtility() {
    }

    /**
     * The logger instance for logging messages.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BlockUtility.class);

    /**
     * Rewrites the blocks in a file to recover data.
     *
     * @param file      The file containing the dirty blocks.
     * @param valueSize The size of the value in each block.
     * @return The new file with recovered data.
     * @throws IOException If an I/O error occurs.
     */
    public static File rewriteBlocks(File file, int valueSize) throws IOException {
        LOG.info("Attempt to recover data in {}", file);
        File recoveredFile = new File(file.getParentFile(), file.getName() + ".recovered");
        byte[] syncMarker = Buffer.getSyncMarker(valueSize);
        LOG.info("Get sync marker - {}", syncMarker);
        long blocksRecovered = 0;

        try (
                RandomAccessFile in = new RandomAccessFile(file, "r");
                DataOutputStream out =
                        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(recoveredFile,
                                false)))
        ) {
            ArrayDeque<Byte> syncMarkerBuffer = new ArrayDeque<>(syncMarker.length);
            CRC32 crc = new CRC32();
            byte[] data = new byte[Config.RECORDS_PER_BLOCK * (valueSize + Config.KEY_SIZE)];

            int bytesRead;
            while ((bytesRead = in.read(data)) == data.length) {
                if (syncMarkerBuffer.size() == syncMarker.length && ByteUtility.arraysEqual(syncMarker,
                        syncMarkerBuffer)) {
                    crc.reset();
                    crc.update(data);
                    if (in.readInt() != (int) crc.getValue()) {
                        in.seek(in.getFilePointer() - bytesRead - syncMarker.length - 4 + 1);
                        syncMarkerBuffer.clear();
                        continue;
                    }
                    out.write(syncMarker);
                    out.write(data);
                    out.writeInt((int) crc.getValue());
                    syncMarkerBuffer.clear();
                    blocksRecovered++;
                } else syncMarkerBuffer.add(data[0]);
                System.arraycopy(data, 1, data, 0, bytesRead - 1);
            }
            out.flush();
        }

        LOG.info("Successfully recovered {} blocks from {}", blocksRecovered, file);
        return recoveredFile;
    }

    /**
     * Verifies the blocks in a file and repairs it if necessary.
     *
     * @param file      The file to verify.
     * @param valueSize The size of the value in each block.
     * @return The verified and repaired file.
     * @throws IOException If an I/O error occurs.
     */
    public static File verifyBlocks(File file, int valueSize) throws IOException {
        if (!file.exists() || file.length() == 0) {
            return file;
        }
        int recordSize = valueSize + Config.KEY_SIZE;
        byte[] syncMarker = Buffer.getSyncMarker(valueSize);

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            ArrayDeque<Byte> syncMarkerBuffer = new ArrayDeque<>(syncMarker.length);
            long validBlocks = 0;
            CRC32 crc = new CRC32();
            boolean corrupted = false;

            while (true) {
                if (syncMarkerBuffer.size() != syncMarker.length) {
                    int data = in.read();
                    if (data == -1) {
                        break;
                    }
                    syncMarkerBuffer.add((byte) (data & 0xFF));
                } else {
                    byte[] data = new byte[Config.RECORDS_PER_BLOCK * recordSize];
                    if (!ByteUtility.arraysEqual(syncMarker, syncMarkerBuffer) || in.read(data) != data.length) {
                        corrupted = true;
                        break;
                    }
                    crc.reset();
                    crc.update(data);
                    if (in.readInt() != (int) crc.getValue()) {
                        corrupted = true;
                        break;
                    }

                    validBlocks++;
                    syncMarkerBuffer.clear();
                }
            }

            if (!corrupted && validBlocks * recordSize * Config.RECORDS_PER_BLOCK
                    + validBlocks * (Config.CRC_SIZE + recordSize) != file.length()) {
                corrupted = true;
            }

            if (corrupted) {
                Files.move(rewriteBlocks(file, valueSize).toPath(), file.toPath(),
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                return new File(file.getAbsolutePath());
            }
        }

        return file;
    }
}

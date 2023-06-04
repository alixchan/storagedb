package com.storage.engine.utilities;

import com.storage.engine.Buffer;
import com.storage.engine.Config;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BlockUtilityTest {

    private final Config defaultDbConfig = new Config();

    @Test
    void verifyBlocks() throws IOException {
        int size = 100;
        defaultDbConfig.setValueSize(size);
        createAndFlushBuffer();
        File tmp = createTempFile();
        assertEquals(tmp, BlockUtility.verifyBlocks(tmp, size));
    }

    private void createAndFlushBuffer() throws IOException {
        Buffer buffer = new Buffer(defaultDbConfig, false);
        buffer.add(1, new byte[100], 0);
        try (FileOutputStream out = createFileOutputStream()) {
            buffer.flush(out);
        }
    }

    private FileOutputStream createFileOutputStream() throws IOException {
        File tmp = createTempFile();
        tmp.deleteOnExit();
        return new FileOutputStream(tmp);
    }

    private File createTempFile() throws IOException {
        return Files.createTempFile("stor_", ".dat").toFile();
    }
}

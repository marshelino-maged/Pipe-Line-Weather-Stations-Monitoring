package bitcask;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class HintFile {
    private final Path hintPath;

    public HintFile(Path hintPath) {
        this.hintPath = hintPath;
    }

    public void writeEntry(String key, long offset) throws IOException {
        byte[] keyBytes = key.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + keyBytes.length + 8);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putLong(offset);
        buffer.flip();

        try (FileChannel channel = FileChannel.open(hintPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            channel.write(buffer);
        }
    }

    public Map<String, Long> loadHints() throws IOException {
        Map<String, Long> hintMap = new HashMap<>();

        if (!Files.exists(hintPath)) return hintMap;

        try (FileChannel channel = FileChannel.open(hintPath, StandardOpenOption.READ)) {
            ByteBuffer meta = ByteBuffer.allocate(4);
            while (channel.read(meta) == 4) {
                meta.flip();
                int keyLen = meta.getInt();
                meta.clear();

                ByteBuffer data = ByteBuffer.allocate(keyLen + 8);
                channel.read(data);
                data.flip();

                byte[] keyBytes = new byte[keyLen];
                data.get(keyBytes);
                long offset = data.getLong();

                hintMap.put(new String(keyBytes), offset);
            }
        }

        return hintMap;
    }
}

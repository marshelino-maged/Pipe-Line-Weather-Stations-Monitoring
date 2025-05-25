package bitcask;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SegmentFile {
    private final Path path;
    private final FileChannel channel;
    private long currentOffset;

    public SegmentFile(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        this.currentOffset = channel.size();
    }

    public synchronized long append(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value.getBytes();

        ByteBuffer buffer = ByteBuffer.allocate(8 + keyBytes.length + valueBytes.length);
        buffer.putInt(keyBytes.length);
        buffer.putInt(valueBytes.length);
        buffer.put(keyBytes);
        buffer.put(valueBytes);
        buffer.flip();

        long offset = currentOffset;
        channel.write(buffer);
        currentOffset += buffer.capacity();
        return offset;
    }

    public synchronized Record read(long offset) throws IOException {
        channel.position(offset);
        ByteBuffer meta = ByteBuffer.allocate(8);
        channel.read(meta);
        meta.flip();
        int keyLen = meta.getInt();
        int valueLen = meta.getInt();

        ByteBuffer data = ByteBuffer.allocate(keyLen + valueLen);
        channel.read(data);
        data.flip();

        byte[] keyBytes = new byte[keyLen];
        data.get(keyBytes);
        byte[] valueBytes = new byte[valueLen];
        data.get(valueBytes);

        return new Record(new String(keyBytes), new String(valueBytes));
    }

    public Path getPath() {
        return path;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void close() throws IOException {
        channel.close();
    }

    public static class Record {
        public final String key;
        public final String value;

        public Record(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}

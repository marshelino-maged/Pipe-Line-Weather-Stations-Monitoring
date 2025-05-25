package bitcask;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class BitCaskEngine {
    private final Path dataDir;
    private final long segmentSizeLimit;
    private final ReentrantLock lock = new ReentrantLock();

    private FileChannel activeSegment;
    private FileChannel activeHint;
    private Path activeSegmentPath;
    private Path activeHintPath;
    private long currentOffset = 0;
    private int segmentIndex = 0;

    private final Map<String, IndexEntry> index = new HashMap<>();

    public BitCaskEngine(String dir, long segmentSizeLimit) throws IOException {
        this.dataDir = Paths.get(dir);
        this.segmentSizeLimit = segmentSizeLimit;
        Files.createDirectories(dataDir);
        initSegments();
        index.putAll(new IndexManager(dataDir).loadFromHintFiles());
    }

    private void initSegments() throws IOException {
        // Start from 0 and keep incrementing until the file doesn't exist
        while (true) {
            Path candidatePath = dataDir.resolve(segmentIndex + ".data");
            if (!Files.exists(candidatePath)) {
                break; // found unused index
            }
            segmentIndex++;
        }
        openNewSegment();
    }


    private void openNewSegment() throws IOException {
        activeSegmentPath = dataDir.resolve(segmentIndex + ".data");
        activeHintPath = dataDir.resolve(segmentIndex + ".hint");
        activeSegment = FileChannel.open(activeSegmentPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        activeHint = FileChannel.open(activeHintPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        currentOffset = activeSegment.size();
    }

    public void put(String key, String value) throws IOException {
        lock.lock();
        try {
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = value.getBytes();

            ByteBuffer buffer = ByteBuffer.allocate(8 + keyBytes.length + valueBytes.length);
            buffer.putInt(keyBytes.length);
            buffer.putInt(valueBytes.length);
            buffer.put(keyBytes);
            buffer.put(valueBytes);
            buffer.flip();

            if (currentOffset + buffer.remaining() > segmentSizeLimit) {
                segmentIndex++;
                openNewSegment();
            }

            activeSegment.write(buffer);
            long offset = currentOffset;
            currentOffset += buffer.capacity();

            ByteBuffer hintBuffer = ByteBuffer.allocate(4 + keyBytes.length + 8);
            hintBuffer.putInt(keyBytes.length);
            hintBuffer.put(keyBytes);
            hintBuffer.putLong(offset);
            hintBuffer.flip();
            activeHint.write(hintBuffer);

            index.put(key, new IndexEntry(activeSegmentPath, offset, valueBytes.length));

        } finally {
            lock.unlock();
        }
    }

    public String get(String key) throws IOException {
        lock.lock();
        try {
            IndexEntry entry = index.get(key);
            if (entry == null) return null;

            try (FileChannel channel = FileChannel.open(entry.path, StandardOpenOption.READ)) {
                channel.position(entry.offset);
                ByteBuffer meta = ByteBuffer.allocate(8);
                channel.read(meta);
                meta.flip();
                int keyLen = meta.getInt();
                int valueLen = meta.getInt();

                ByteBuffer kv = ByteBuffer.allocate(keyLen + valueLen);
                channel.read(kv);
                kv.flip();

                byte[] keyBytes = new byte[keyLen];
                kv.get(keyBytes);
                byte[] valueBytes = new byte[valueLen];
                kv.get(valueBytes);
                return new String(valueBytes);
            }

        } finally {
            lock.unlock();
        }
    }

    public Map<String, String> dumpAll() throws IOException {
        lock.lock();
        try {
            Map<String, String> result = new HashMap<>();
            for (String key : index.keySet()) {
                String value = get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public void compact() throws IOException {
        Map<String, SegmentFile.Record> latestRecords = new HashMap<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.data")) {
            for (Path segmentPath : stream) {
                SegmentFile segment = new SegmentFile(segmentPath);
                long offset = 0;
                while (offset < segment.getCurrentOffset()) {
                    SegmentFile.Record record = segment.read(offset);
                    latestRecords.put(record.key, record);

                    int keyLen = record.key.getBytes().length;
                    int valLen = record.value.getBytes().length;
                    offset += 8 + keyLen + valLen;
                }
                segment.close();
            }
        }

        int compactedIndex = getNextSegmentIndex();
        Path compactedPath = dataDir.resolve(compactedIndex + ".data");
        Path compactedHintPath = dataDir.resolve(compactedIndex + ".hint");

        SegmentFile compactedSegment = new SegmentFile(compactedPath);
        HintFile hintWriter = new HintFile(compactedHintPath);

        for (Map.Entry<String, SegmentFile.Record> entry : latestRecords.entrySet()) {
            long offset = compactedSegment.append(entry.getKey(), entry.getValue().value);
            hintWriter.writeEntry(entry.getKey(), offset);
            index.put(entry.getKey(), new IndexEntry(compactedPath, offset, entry.getValue().value.length()));
        }

        compactedSegment.close();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, path -> path.toString().endsWith(".data") || path.toString().endsWith(".hint"))) {
            for (Path oldPath : stream) {
                if (!oldPath.equals(compactedPath) && !oldPath.equals(compactedHintPath)) {
                    Files.deleteIfExists(oldPath);
                }
            }
        }
    }

    private int getNextSegmentIndex() throws IOException {
        int max = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.data")) {
            for (Path path : stream) {
                String name = path.getFileName().toString().replace(".data", "");
                try {
                    int idx = Integer.parseInt(name);
                    max = Math.max(max, idx);
                } catch (NumberFormatException ignored) {}
            }
        }
        return max + 1;
    }

    static class IndexEntry {
        Path path;
        long offset;
        int valueSize;

        public IndexEntry(Path path, long offset, int valueSize) {
            this.path = path;
            this.offset = offset;
            this.valueSize = valueSize;
        }
    }
}

package test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Map;

public class BitCaskInspector {

    public static void printHintFile(Path hintPath) throws IOException {
        System.out.println("📄 HINT FILE (Basic Format): " + hintPath.getFileName());

        try (FileChannel channel = FileChannel.open(hintPath, StandardOpenOption.READ)) {
            ByteBuffer meta = ByteBuffer.allocate(4); // key length

            while (channel.read(meta) == 4) {
                meta.flip();
                int keyLen = meta.getInt();
                meta.clear();

                ByteBuffer data = ByteBuffer.allocate(keyLen + 8); // key + offset
                if (channel.read(data) != keyLen + 8) break;

                data.flip();
                byte[] keyBytes = new byte[keyLen];
                data.get(keyBytes);
                long offset = data.getLong();

                String key = new String(keyBytes);
                System.out.printf("Key: %-20s | Offset: %-8d%n", key, offset);
            }
        }
    }


    public static void printDataFile(Path dataPath) throws IOException {
        System.out.println("📄 DATA FILE: " + dataPath.getFileName());

        try (FileChannel channel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            ByteBuffer meta = ByteBuffer.allocate(8); // key length + value length

            while (channel.read(meta) == 8) {
                meta.flip();
                int keyLen = meta.getInt();
                int valueLen = meta.getInt();
                meta.clear();

                ByteBuffer data = ByteBuffer.allocate(keyLen + valueLen);
                if (channel.read(data) != keyLen + valueLen) break;

                data.flip();
                byte[] keyBytes = new byte[keyLen];
                data.get(keyBytes);
                byte[] valueBytes = new byte[valueLen];
                data.get(valueBytes);

                String key = new String(keyBytes);
                String value = new String(valueBytes);
                System.out.printf("Key: %-20s | Value: %-20s%n", key, value);
            }
        }
    }

    public static void main(String[] args) throws IOException {

        Path dataPath = Paths.get("data/segments/0.data");
        Path hintPath = Paths.get("data/segments/0.hint");

        printDataFile(dataPath);
        System.out.println();
        printHintFile(hintPath);
    }
}

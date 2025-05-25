package test;

import bitcask.BitCaskEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class BitCaskCompactionRunner {

    public static void main(String[] args) {
        try {
            String directory = "data/segments"; // 🔁 adjust to your path if needed
            BitCaskEngine engine = new BitCaskEngine(directory, 128, true); // small segment size to trigger rotation

            System.out.println("🔍 Records before compaction:");
            Map<String, String> before = engine.dumpAll();
            before.forEach((k, v) -> System.out.println("  " + k + " = " + v));

            System.out.println("\n🧹 Running compaction...");
            engine.compact();

            System.out.println("\n✅ Records after compaction:");
            Map<String, String> after = engine.dumpAll();
            after.forEach((k, v) -> System.out.println("  " + k + " = " + v));

            // Optional: show current files
            System.out.println("\n📂 Remaining segment files:");
            Files.list(Path.of(directory))
                .filter(Files::isRegularFile)
                .forEach(p -> System.out.println("  - " + p.getFileName()));

        } catch (IOException e) {
            System.err.println("❌ Compaction failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

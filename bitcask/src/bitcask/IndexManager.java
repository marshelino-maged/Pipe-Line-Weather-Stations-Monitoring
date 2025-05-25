package bitcask;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class IndexManager {
    private final Path dataDir;
    private final Map<String, BitCaskEngine.IndexEntry> index;

    public IndexManager(Path dataDir) {
        this.dataDir = dataDir;
        this.index = new HashMap<>();
    }

    public Map<String, BitCaskEngine.IndexEntry> loadFromHintFiles() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.hint")) {
            for (Path hintPath : stream) {
                HintFile hintFile = new HintFile(hintPath);
                Map<String, Long> hintMap = hintFile.loadHints();
                String segmentFileName = hintPath.getFileName().toString().replace(".hint", ".data");
                Path segmentPath = dataDir.resolve(segmentFileName);

                for (Map.Entry<String, Long> entry : hintMap.entrySet()) {
                    // We don't know value size from hint, set to -1 (or load on demand)
                    index.put(entry.getKey(), new BitCaskEngine.IndexEntry(segmentPath, entry.getValue(), -1));
                }
            }
        }
        return index;
    }

    public Map<String, BitCaskEngine.IndexEntry> getIndex() {
        return index;
    }
}

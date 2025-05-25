package bitcask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class BitCaskWriter {
    BitCaskEngine bitCaskEngine;

    public BitCaskWriter() throws IOException {
        String bitCaskDirectory = System.getenv().getOrDefault("BITCASK_DIRECTORY", "/bitcask");
        int bitCaskMaxBytes = Integer.parseInt(System.getenv().getOrDefault("BITCASK_MAX_BYTES", "4096"));
        bitCaskEngine = new BitCaskEngine(bitCaskDirectory, bitCaskMaxBytes, false);
    }

    public void dumpMessage(String jsonMessage) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(jsonMessage);
        String key = node.get("station_id").asText();

        bitCaskEngine.put(key, jsonMessage);
    }
}

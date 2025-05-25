package bitcask;

import java.io.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.util.Map;
import java.io.InputStream;

public class TestBitCask {
    public static void main(String[] args) throws Exception {
        BitCaskEngine engine = new BitCaskEngine("data/segments", 1024 * 1024);

        // Load mock data
        JSONArray readings;
        try (InputStream is = new FileInputStream("src/bitcask/weather_mock_data.json")) {
            readings = new JSONArray(new JSONTokener(is));
        }

        // Write mock data
        for (int i = 0; i < readings.length(); i++) {
            JSONObject obj = readings.getJSONObject(i);
            String stationId = String.valueOf(obj.getInt("s_no"));

            // Construct a combined object to store
            JSONObject combined = new JSONObject();
            combined.put("station_id", obj.getInt("station_id"));
            combined.put("battery_status", obj.getString("battery_status"));
            combined.put("status_timestamp", obj.getLong("status_timestamp"));
            combined.put("weather", obj.getJSONObject("weather"));

            // Store it as a string
            String value = combined.toString();
            engine.put(stationId, value);

            System.out.println("PUT: s_no=" + stationId + ", value=" + value);
        }


        // Simulate shutdown and recovery
        engine = null;
        System.gc();

        BitCaskEngine recovered = new BitCaskEngine("data/segments", 1024 * 1024);
        System.out.println("Recovered value for station 1: " + recovered.get("1"));
        System.out.println("Recovered value for station 2: " + recovered.get("2"));

        // Dump all current keys
        System.out.println("\n--- Dumping all current key-values ---");
        for (Map.Entry<String, String> entry : recovered.dumpAll().entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        // Schedule compaction test
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                recovered.compact();
                System.out.println("[Compactor] Compaction completed.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            System.out.println("Read during compaction (station 1): " + recovered.get("1"));
        }
    }
}

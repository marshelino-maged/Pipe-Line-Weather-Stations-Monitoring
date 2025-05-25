import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import java.util.*;

public class ParquetWriter {
    private final String parquetDirectory;
    private final WeatherDataArchiver dataArchiver;
    private final Schema schema;
    private final Map<Integer, List<String>> jsonDataMap = new HashMap<>();

    private static final String WEATHER_SCHEMA = """
        {
          "namespace": "com.example.weather",
          "type": "record",
          "name": "WeatherRecord",
          "fields": [
            {"name": "station_id", "type": "int"},
            {"name": "s_no", "type": "int"},
            {"name": "battery_status", "type": "string"},
            {"name": "status_timestamp", "type": "long"},
            {
              "name": "weather",
              "type": {
                "type": "record",
                "name": "WeatherDetails",
                "fields": [
                  {"name": "humidity", "type": "int"},
                  {"name": "temperature", "type": "int"},
                  {"name": "wind_speed", "type": "int"}
                ]
              }
            }
          ]
        }
        """;

    public ParquetWriter() {
        this.parquetDirectory = System.getenv().getOrDefault("PARQUET_DIRECTORY", "/parquet");
        this.dataArchiver = new WeatherArchiverImpl();
        this.schema = new Schema.Parser().parse(WEATHER_SCHEMA);
    }

    public void dumpMessage(String jsonMessage) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(jsonMessage);

        int stationId = node.get("station_id").asInt();

        jsonDataMap.putIfAbsent(stationId, new ArrayList<>());
        jsonDataMap.get(stationId).add(jsonMessage);

        if (jsonDataMap.get(stationId).size() >= 100) {
            String jsonString = getJsonString(jsonDataMap.get(stationId));
            dataArchiver.WriteParquet(jsonString, parquetDirectory, schema);
            jsonDataMap.get(stationId).clear();
        }
    }

    private String getJsonString(List<String> jsonList) {
        return "[" + String.join(",", jsonList) + "]";
    }
}

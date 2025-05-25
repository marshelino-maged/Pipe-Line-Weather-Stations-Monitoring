import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WeatherArchiverImpl implements WeatherDataArchiver {

    private static final Random random = new Random();
    /**
     * Loads an Avro schema from a given file path.
     *
     * @param schemaFilePath The path to the Avro schema file (in .avsc format).
     * @return The parsed Avro Schema object.
     * @throws Exception If the schema file cannot be read or parsed.
     */
    @Override
    public Schema loadSchema(String schemaFilePath) throws Exception {
        return new Schema.Parser().parse(new File(schemaFilePath));
    }
    /**
     * Converts JSON data to Avro GenericRecords using the provided schema,
     * partitions the records, and writes them to Parquet files in the specified base directory.
     *
     * @param JsonData The JSON string containing an array of records to be written.
     * @param baseDir  The base directory where the Parquet partition directories will be created.
     * @param schema   The Avro schema describing the structure of the JSON records.
     * @throws Exception If there is an error during JSON parsing, record generation, or writing to Parquet.
     */
    @Override
    public void WriteParquet(String JsonData, String baseDir , Schema schema) throws Exception {

        Map<String, List<GenericRecord>> partitions = generateRecords(schema, JsonData);
        writePartitionsToParquet(partitions, schema, baseDir);
    }


    /**
     * Parses JSON input and converts it into Avro GenericRecords, grouped by partition keys.
     * Each record must conform to the provided Avro schema.
     *
     * @param schema   The Avro schema defining the structure of each record,
     *                 including a nested "weather" field.
     * @param jsonData A JSON string representing an array of records.
     *                 Each record should match the structure defined by the schema.
     * @return A map where each key is a partition string in the format
     *         "station_id=<id>/time=<formatted_time>", and the value is a list of
     *         GenericRecords belonging to that partition.
     * @throws IOException If the JSON parsing fails or the data doesn't match the schema.
     */

    private Map<String, List<GenericRecord>> generateRecords(Schema schema, String jsonData) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonData);

        Schema weatherSchema = schema.getField("weather").schema();
        Map<String, List<GenericRecord>> partitions = new HashMap<>();

        for (JsonNode node : root) {
            long stationId = node.get("station_id").asLong();
            long s_no = node.get("s_no").asLong();
            long timestamp = node.get("status_timestamp").asLong();
            String batteryStatus = node.get("battery_status").asText();
            String timePartition = formatTimePartition(timestamp);

            JsonNode weatherNode = node.get("weather");
            int humidity = weatherNode.get("humidity").asInt();
            int temperature = weatherNode.get("temperature").asInt();
            int windSpeed = weatherNode.get("wind_speed").asInt();

            GenericRecord weatherRecord = new GenericData.Record(weatherSchema);
            weatherRecord.put("humidity", humidity);
            weatherRecord.put("temperature", temperature);
            weatherRecord.put("wind_speed", windSpeed);

            GenericRecord record = new GenericData.Record(schema);
            record.put("station_id", stationId);
            record.put("s_no", s_no);
            record.put("battery_status", batteryStatus);
            record.put("status_timestamp", timestamp);
            record.put("weather", weatherRecord);

            String key = "station_id=" + stationId + "/time=" + timePartition;
            partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
        }
        return  partitions;

    }

    /**
     * Writes grouped weather records into partitioned Parquet files using the provided Avro schema.
     *
     * Each partition corresponds to a specific combination of station ID and timestamp hour (e.g., station_id=1/time=2025-05-25-15),
     * and is stored in a separate directory under the specified base directory.
     *
     * The files are written using the Snappy compression codec and include all the records belonging to each partition.
     *
     * @param partitions A map where each key is a partition string and the value is a list of Avro generic records.
     * @param schema The Avro schema that defines the structure of the records.
     * @param baseDir The base directory where partition folders and Parquet files will be created.
     * @throws Exception If an I/O error occurs during file writing.
     */

    private void writePartitionsToParquet(Map<String, List<GenericRecord>> partitions, Schema schema, String baseDir) throws Exception {
        for (Map.Entry<String, List<GenericRecord>> entry : partitions.entrySet()) {
            String partition = entry.getKey();
            List<GenericRecord> records = entry.getValue();

            String dirPath = baseDir + "/" + partition;
            new File(dirPath).mkdirs();

            String filePath = dirPath + "/weather_data_" + System.currentTimeMillis() + ".parquet";

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(new Path(filePath))
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {

                for (GenericRecord record : records) {
                    writer.write(record);
                }
            }

            System.out.println("Wrote " + records.size() + " records to: " + filePath);
        }
    }

    private static String formatTimePartition(long epochSeconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
    }

    private static String getRandomBatteryStatus() {
        int r = random.nextInt(100);
        if (r < 30) return "low";
        else if (r < 70) return "medium";
        else return "high";
    }
}

//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.GenericRecordBuilder;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.parquet.avro.AvroParquetWriter;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//
//import java.io.File;
//import java.time.Duration;
//import java.util.Collections;
//import java.util.Properties;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//
//public class CentraStation {
//
//    private static final String TOPIC = "weather_topic";
//    private static final String OUTPUT_DIR = "parquet_output";
//
//    // Avro schema definition for weather record
//    private static final String WEATHER_SCHEMA = """
//            {
//              "namespace": "com.example.weather",
//              "type": "record",
//              "name": "Weather",
//              "fields": [
//                {"name": "stationId", "type": "string"},
//                {"name": "temperature", "type": "double"},
//                {"name": "humidity", "type": "int"},
//                {"name": "timestamp", "type": "long"}
//              ]
//            }
//            """;
//
//    public static void main(String[] args) throws Exception {
//        // Create output directory if not exists
//        File outDir = new File(OUTPUT_DIR);
//        if (!outDir.exists()) outDir.mkdirs();
//
//        // Setup Kafka consumer properties
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "kafka-service.default.svc.cluster.local:9092");
//        props.put("group.id", "central_station_consumer_group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");
//
//        Schema schema = new Schema.Parser().parse(WEATHER_SCHEMA);
//
//        // Create Kafka consumer
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
//            consumer.subscribe(Collections.singletonList(TOPIC));
//
//            // Prepare parquet writer (rolling by simple count example)
//            int fileIndex = 0;
//            int recordsPerFile = 1000;
//            int recordCount = 0;
//
//            ParquetWriter<GenericRecord> writer = createParquetWriter(schema, fileIndex);
//
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//                if (records.isEmpty()) continue;
//
//                for (ConsumerRecord<String, String> record : records) {
//                    // Assuming JSON string value, parse it manually or use your favorite JSON lib
//                    GenericRecord weatherRecord = parseWeatherJson(record.value(), schema);
//                    writer.write(weatherRecord);
//
//                    recordCount++;
//                    if (recordCount >= recordsPerFile) {
//                        writer.close();
//                        fileIndex++;
//                        writer = createParquetWriter(schema, fileIndex);
//                        recordCount = 0;
//                    }
//                }
//                consumer.commitSync();
//            }
//        }
//    }
//
//    private static ParquetWriter<GenericRecord> createParquetWriter(Schema schema, int fileIndex) throws Exception {
//        Path path = new Path(OUTPUT_DIR + "/weather_" + fileIndex + ".parquet");
//        return AvroParquetWriter.<GenericRecord>builder(path)
//                .withSchema(schema)
//                .withConf(new Configuration())
//                .withCompressionCodec(CompressionCodecName.SNAPPY)
//                .withPageSize(4 * 1024 * 1024)
//                .build();
//    }
//
//    private static GenericRecord parseWeatherJson(String json, Schema schema) {
//        // Simple manual parsing (assuming fixed format JSON), replace with Jackson/Gson for production
//
//        // Example input: {"stationId":"station-1","temperature":25.6,"humidity":75,"timestamp":1685000000000}
//
//        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
//
//        String s = json.replaceAll("[{}\"]", "");
//        String[] parts = s.split(",");
//        for (String part : parts) {
//            String[] kv = part.split(":");
//            String key = kv[0].trim();
//            String value = kv[1].trim();
//
//            switch (key) {
//                case "stationId" -> builder.set("stationId", value);
//                case "temperature" -> builder.set("temperature", Double.parseDouble(value));
//                case "humidity" -> builder.set("humidity", Integer.parseInt(value));
//                case "timestamp" -> builder.set("timestamp", Long.parseLong(value));
//            }
//        }
//        return builder.build();
//    }
//}

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CentralStation {
    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BROKER_HOST", "kafka-service:9092");
        String groupId = System.getenv().getOrDefault("KAFKA_CONSUMER_GROUP_ID", "weather-consumer-group");
        String topic = System.getenv().getOrDefault("KAFKA_CONSUMER_TOPIC", "weather_topic");
        String invalidTopic = System.getenv().getOrDefault("KAFKA_INVALID_TOPIC", "invalid_weather_topic");

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Consuming messages from topic: " + topic);

            ParquetWriter parquetWriter = new ParquetWriter();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    parquetWriter.dumpMessage(record.value());
                    System.out.printf("Received: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
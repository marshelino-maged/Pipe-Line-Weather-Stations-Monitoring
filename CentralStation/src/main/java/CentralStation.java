import bitcask.BitCaskWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import parquet.ParquetWriter;

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

            BitCaskWriter bitCaskWriter = new BitCaskWriter();
            ParquetWriter parquetWriter = new ParquetWriter();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    bitCaskWriter.dumpMessage(record.value());
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

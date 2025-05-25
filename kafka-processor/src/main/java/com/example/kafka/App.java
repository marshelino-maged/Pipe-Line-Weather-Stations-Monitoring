package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class App {
    public static void main(String[] args) {

        String rainTopic = System.getenv("KAFKA_WRITE_TOPIC");
        String invalidTopic = System.getenv("KAFKA_INVALID_TOPIC");

        Properties rainProps = new Properties();
        rainProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        rainProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BROKER"));
        rainProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        rainProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Properties invalidProps = new Properties();
        // invalidProps.put(StreamsConfig.APPLICATION_ID_CONFIG,
        // "invalid-detector-app");
        invalidProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BROKER"));
        invalidProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        invalidProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(invalidProps);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(System.getenv("KAFKA_READ_TOPIC"));

        KStream<String, String> alerts = stream
                .filter((key, value) -> {
                    try {
                        JSONObject json = new JSONObject(value.replace("\\", ""));
                        JSONObject weather = json.getJSONObject("weather");
                        double humidity = weather.getDouble("humidity");
                        return humidity > 70.0;
                    } catch (Exception e) {
                        // Handle Error:
                        producer.send(new ProducerRecord<String, String>(invalidTopic, value));
                        // System.out.println("Key: " + key + ", Value: " + value);
                        System.out.println("Error parsing JSON: " + e.getMessage());
                        return false;
                    }
                })
                .mapValues(value -> {
                    try {
                        JSONObject json = new JSONObject(value);
                        String result = "{\"alert\": \"Rain detected\", \"station_id\": "
                                + json.getLong("station_id") + ", \"status_timestamp\": "
                                + json.getLong("status_timestamp") + "}";
                        System.out.println("Alert generated: " + result);
                        return result;
                    } catch (Exception e) {
                        producer.send(new ProducerRecord<String, String>(invalidTopic, value));
                        System.out.println("Error generating alert: " + e.getMessage());
                        return "{\"alert\": \"Rain detected\"}";
                    }
                });

        alerts.to(rainTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), rainProps);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            producer.close();
        }));
    }
}

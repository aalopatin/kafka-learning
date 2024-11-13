package com.github.alopatin.kafka_learning.java.producer_json;

import com.github.alopatin.kafka_learning.java.producer_json.model.iot.LocationMeasurementJson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

public class Main {

    private static final Config config = ConfigFactory.load();

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Count of arguments should be 2");
        }

        var topic = args[0];
        var count = Integer.parseInt(args[1]);

        var kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"));
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        kafkaProperties.put("schema.registry.url", "http://localhost:8081");

        var locations = new String[]{"room 1", "room 2", "room 3", "room 4", "room 5"};

        var interval = Optional.ofNullable(
                switch (topic) {
                    case "temperature" -> new double[]{15.0, 30.0};
                    case "humidity" -> new double[]{30.0, 70.0};
                    default -> null;
                }
        ).orElseThrow(() -> new IllegalArgumentException("Topic should be either 'temperature' or 'humidity'"));

        var rand = new Random();
        try (
                var producer = new KafkaProducer<String, LocationMeasurementJson>(kafkaProperties)
        ) {
            java.util.stream.IntStream.rangeClosed(1, count).forEach(i -> {
                var key = LocalDateTime.now().toString();

                var location = locations[rand.nextInt(locations.length)];
                var measurement = Math.round((rand.nextDouble() * (interval[1] - interval[0]) + interval[0]) * 10.0) / 10.0;

                var value = new LocationMeasurementJson(location, measurement);
                var record = new ProducerRecord<>(topic + "-json", key, value);

                System.out.printf("key: %s, value: %s\n", key, value);

                try {
                    producer.send(record);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            });
        }
    }
}

package com.github.alopatin.kafka_learning.java.producer_simple;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
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
        kafkaProperties.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"));
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put("value.serializer", DoubleSerializer.class.getName());

        var interval = Optional.ofNullable(
                switch (topic) {
                    case "temperature" -> new double[]{15.0, 30.0};
                    case "humidity" -> new double[]{30.0, 70.0};
                    default -> null;
                }
        ).orElseThrow(() -> new IllegalArgumentException("Topic should be either 'temperature' or 'humidity'"));

        var rand = new Random();
        try (var producer = new KafkaProducer<String, Double>(kafkaProperties)) {
            java.util.stream.IntStream.rangeClosed(1, count).forEach(i -> {
                var key = LocalDateTime.now().toString();
                var value = Math.round((rand.nextDouble() * (interval[1] - interval[0]) + interval[0]) * 10.0) / 10.0;
                var record = new ProducerRecord<>(topic, key, value);

                System.out.printf("key: %s, value: %.1f%n", key, value);

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

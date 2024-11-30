package com.github.alopatin.kafka_learning.scala.consumer_avro

import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties}

/**
 * Exersice: Write Kafka consumer with Avro deserializer
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-json")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    kafkaProperties.put("schema.registry.url", config.getString("kafka.schema.registry.url"))

    val consumer = new KafkaConsumer[String, GenericData.Record](kafkaProperties)
    consumer.subscribe(Collections.singletonList(s"$topic-avro"))

    val timeout = Duration.ofMillis(100)

    while (true) {
      val records = consumer.poll(timeout)
      records.forEach(println(_))
    }

  }
}

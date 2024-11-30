package com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer

import com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer.model.iot.LocationMeasurement
import com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer.serialization.iot.LocationMeasurementDeserializer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

/**
 * Exersice: Write Kafka consumer with custom deserializer
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-custom")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LocationMeasurementDeserializer].getName)

    val consumer = new KafkaConsumer[String, LocationMeasurement](kafkaProperties)
    consumer.subscribe(Collections.singletonList(s"$topic-custom"))

    val timeout = Duration.ofMillis(100)

    while (true) {
      val records = consumer.poll(timeout)
      records.forEach(println(_))
    }

  }
}

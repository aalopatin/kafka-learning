package com.github.alopatin.kafka_learning.scala.consumer_standalone

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{DoubleDeserializer, StringDeserializer}

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

/**
 * Exersice: Write Kafka consumer that consumes message from start of the partition every time.
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_standalone")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val consumer = new KafkaConsumer[String, String](kafkaProperties)

    val partitions = consumer.partitionsFor(topic).asScala

    consumer.assign(
        partitions
        .map(
          info => new TopicPartition(info.topic(), info.partition())
        ).toList.asJava
    )

    val timeout = Duration.ofMillis(100)
    while (true) {
      val records = consumer.poll(timeout)
      records.forEach(println(_))
    }

  }
}

package com.github.alopatin.kafka_learning.scala.consumer_start_subscribe

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
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-start")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val timeout = Duration.ofMillis(100)

    val consumer = new KafkaConsumer[String, String](kafkaProperties)
    consumer.subscribe(Collections.singletonList(topic))

    var partitions = consumer.assignment()

    while (partitions.isEmpty) {
      consumer.poll(timeout)
      partitions = consumer.assignment()
    }

    consumer.seekToBeginning(partitions)

    while (true) {
      val records = consumer.poll(timeout)
      records.forEach(println(_))
    }

  }
}

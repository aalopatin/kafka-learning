package com.github.alopatin.kafka_learning.scala.consumer_start_assign

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

    val consumer = new KafkaConsumer[String, String](kafkaProperties)

    // Manually assign partitions
    // We need to use manual assignment because consumer isn't assigned to any partitions
    // before partitions have new messages to poll
    // This solution can be tricky because we can't use Kafka dynamic partition assigment mechanism in this situation.
    // another solution is to poll messages until we get some and after that call seekToBeginning method (take a look at consumer-start-subscribe module)
    consumer.assign(
      consumer
        .partitionsFor(topic)
        .asScala
        .map(
          info => new TopicPartition(info.topic(), info.partition())
        ).toList.asJava
    )

    val partitions = consumer.assignment()
    consumer.seekToBeginning(partitions)

    val timeout = Duration.ofMillis(100)
    while (true) {
      val records = consumer.poll(timeout)
      records.forEach(println(_))
    }

  }
}

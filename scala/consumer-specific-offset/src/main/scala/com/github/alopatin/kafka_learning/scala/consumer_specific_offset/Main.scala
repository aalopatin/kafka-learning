package com.github.alopatin.kafka_learning.scala.consumer_specific_offset

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

/**
 * Exersice: Write Kafka consumer that commits specified offset, for example, for every N messages, or even for every message.
 */
object Main {

  private val config = ConfigFactory.load()
  private val currentOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
  private var count = 0

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-specific-offset")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = new KafkaConsumer[String, String](kafkaProperties)
    consumer.subscribe(Collections.singletonList(topic))

    val timeout = Duration.ofMillis(100)

    while (true) {
      val records = consumer.poll(timeout)

      records.forEach(record => {
        println(record)
        count += 1
        currentOffsets.put(
          new TopicPartition(record.topic(), record.partition()),
          new OffsetAndMetadata(record.offset() + 1)
        )
        // We will commit every 5 messages
        if (count % 5 == 0) {
          println(s"Committed count: $count")
          println(s"Current offsets: $currentOffsets")
          consumer.commitAsync(currentOffsets, null)
        }
      })
    }
  }
}

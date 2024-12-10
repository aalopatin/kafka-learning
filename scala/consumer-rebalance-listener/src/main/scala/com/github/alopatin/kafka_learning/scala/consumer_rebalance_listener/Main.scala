package com.github.alopatin.kafka_learning.scala.consumer_rebalance_listener

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util

/**
 * Exersice: Write Kafka consumer that commits current offset when rebalance occurs by using consumer rebalance listener.
 */
object Main {

  private val config = ConfigFactory.load()
  private val currentOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new util.Properties()
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-rebalance-listener")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5)

    val consumer = new KafkaConsumer[String, String](kafkaProperties)

    consumer.subscribe(util.Collections.singletonList(topic), new CustomRebalanceListener(consumer, currentOffsets))

    val timeout = Duration.ofMillis(100)

    while (true) {
      val records = consumer.poll(timeout)

      if (!records.isEmpty) {
        println("Poll")
        records.forEach(record => {
          println(record)
          currentOffsets.put(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
          )
        })
      }
      consumer.commitAsync(currentOffsets, null)
    }

  }


}

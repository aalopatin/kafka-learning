package com.github.alopatin.kafka_learning.scala.consumer_sync

import com.sun.org.slf4j.internal.LoggerFactory
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{DoubleDeserializer, StringDeserializer}

import java.time.Duration
import java.util.{Collections, Properties}

/**
 * Exersice: Write Kafka consumer that commits current offset synchronously.
 */
object Main {

  private val config = ConfigFactory.load()

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-sync")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = new KafkaConsumer[String, String](kafkaProperties)
    consumer.subscribe(Collections.singletonList(topic))

    val timeout = Duration.ofMillis(100)

    var n = 1
    while (true) {
      val records = consumer.poll(timeout)
      if (!records.isEmpty) {
        println(s"Poll number: $n")
        records.forEach(println(_))
        n += 1
        try {
          consumer.commitSync()
        } catch {
          case e: CommitFailedException => logger.error("Commit failed", e)
        }
      }
    }
  }
}

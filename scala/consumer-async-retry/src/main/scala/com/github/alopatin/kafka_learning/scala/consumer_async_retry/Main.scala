package com.github.alopatin.kafka_learning.scala.consumer_async_retry

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.{Collections, Properties}

/**
 * Exersice: Write Kafka consumer that commits current offset asynchronously and make a retry by using sequence number to check current commit.
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val topic = args(0)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-async-retry1")
    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = new KafkaConsumer[String, String](kafkaProperties)
    consumer.subscribe(Collections.singletonList(topic))

    val timeout = Duration.ofMillis(100)

    val commitSequence = new AtomicLong(0)
    while (true) {
      val records = consumer.poll(timeout)
      if (!records.isEmpty) {
        records.forEach(println(_))
        consumer.commitAsync(new OffsetCommitCallback() {
          private val currentSequence = commitSequence.incrementAndGet()
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            println(s"Current sequence: $currentSequence, sequence: ${commitSequence.get()}")
              if (exception != null) {
                println(s"Commit error: $exception")
                if (currentSequence == commitSequence.get()) {
                  println(s"Attempt to retry.")
                  consumer.commitAsync()
                }
              }
          }
        })
      }
    }
  }
}

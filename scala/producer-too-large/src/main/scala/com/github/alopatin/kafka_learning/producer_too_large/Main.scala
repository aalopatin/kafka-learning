package com.github.alopatin.kafka_learning.producer_too_large

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.LocalDateTime
import java.util.Properties
import scala.util.Random

/**
 * Exersice: Write Kafka producer that generates too long messages and gets rejects from broker
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    // topic with config max.message.bytes=100
    val topic = "short-messages"
    val maxMessageBytes = args(0).toInt

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProperties.put("value.serializer", classOf[StringSerializer].getName)

    val rand = new Random()
    val producer = new KafkaProducer[String, String](kafkaProperties)

    for (len <- 1 to maxMessageBytes) {
      val key = LocalDateTime.now().toString
      val value = new String(rand.alphanumeric.take(len).toArray)
      val record = new ProducerRecord(topic, key, value)

      // print record that we're going to send
      println(record.toString)

      try {
        val metadata = producer.send(record).get()

        // check the size of key and value of our message
        // we should see that broker will start to reject our messages before key + value = 100 bytes
        // because of message overhead that producer adds to messages
        println(s"Key size: ${metadata.serializedKeySize()}")
        println(s"Value size: ${metadata.serializedValueSize()}")
        println("-----------------------------------------------")

      } catch {
        case e: Throwable => throw e
      }

//      sleep(100)
    }

  }
}

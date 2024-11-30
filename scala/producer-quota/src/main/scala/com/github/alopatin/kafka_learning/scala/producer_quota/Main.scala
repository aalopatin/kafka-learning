package com.github.alopatin.kafka_learning.scala.producer_quota

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.LocalDateTime
import java.util.Properties
import scala.util.{Random, Using}

/**
 * Exersice: Write Kafka producer that sent messages with higher rate than quote allows to do it.
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 1, "Count of arguments should be 1")

    val count = args(0).toInt

    val kafkaProperties = new Properties()
    kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "producerWithQuota")
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val rand = new Random()

    Using(new KafkaProducer[String, String](kafkaProperties)) {
      producer =>
        for (_ <- 1 to count) {
          val key = LocalDateTime.now().toString
          //Generate value of 100 bytes in size
          val value = new String(rand.alphanumeric.take(200).toArray)
          val record = new ProducerRecord("producer-quota", key, value)

          println(s"key: $key, value: $value")

          try {
            producer.send(record)
          } catch {
            case e: Throwable => e.printStackTrace()
          }

        }
    }

  }
}

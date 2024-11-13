package com.github.alopatin.kafka_learning.scala.producer_async

import com.github.alopatin.kafka_learning.scala.producer_async.callbacks.ProducerCallbackPrintException
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{DoubleSerializer, StringSerializer}

import java.lang.Thread.sleep
import java.time.LocalDateTime
import java.util.Properties
import scala.util.Random

/**
 * Exersice: Write sync Kafka producer that produce messages and process an answer from broker synchronously.
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 2, "Count of arguments should be 2")

    val topic = args(0)
    val count = args(1).toInt

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProperties.put("value.serializer", classOf[DoubleSerializer].getName)

    val interval = topic match {
      case "temperature" => (15.0, 30.0)
      case "humidity" => (30.0, 70.0)
      case _ => throw new IllegalArgumentException("topic should be one of two: temperature of humidity")
    }

    val rand = new Random()

    val producer = new KafkaProducer[String, Double](kafkaProperties)

    for (_ <- 1 to count) {
      val key = LocalDateTime.now().toString
      val value = (rand.between(interval._1, interval._2) * 10).round / 10.0
      val record = new ProducerRecord(topic, key, value)

      println(s"key: $key, value: $value")

      // To send messages to Kafka asynchronously
      // we need to extend Callback class
      // and implement method onCompletion
      // and put instance of new class to method send
      producer.send(record, new ProducerCallbackPrintException())

      sleep(100)
    }

  }
}

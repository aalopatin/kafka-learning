package com.github.alopatin.kafka_learning.scala.producer_json_case_class

import com.github.alopatin.kafka_learning.scala.producer_json_case_class.model.iot.LocationMeasurement
import com.github.alopatin.kafka_learning.scala.producer_json_case_class.serialization.CustomKafkaJsonSchemaSerializer
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.lang.Thread.sleep
import java.time.LocalDateTime
import java.util.Properties
import scala.util.Random

/**
 * Exersice: Write Kafka producer with json schema serializer
 */
object Main {

  private val config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    assert(args.length == 2, "Count of arguments should be 2")

    val topic = args(0)
    val count = args(1).toInt

    val kafkaProperties = new Properties()
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"))
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[CustomKafkaJsonSchemaSerializer[LocationMeasurement]].getName)
    kafkaProperties.put("schema.registry.url", config.getString("kafka.schema.registry.url"))

    val locations = Array("room 1", "room 2", "room 3", "room 4", "room 5")

    val interval = topic match {
      case "temperature" => (15.0, 30.0)
      case "humidity" => (30.0, 70.0)
      case _ => throw new IllegalArgumentException("topic should be one of two: temperature of humidity")
    }

    val rand = new Random()

    val producer = new KafkaProducer[String, LocationMeasurement](kafkaProperties)

    for (_ <- 1 to count) {
      val key = LocalDateTime.now().toString

      val location = locations(rand.nextInt(locations.length))
      val measurement = (rand.between(interval._1, interval._2) * 10).round / 10.0
      val value = LocationMeasurement(location, measurement)

      val record = new ProducerRecord(s"$topic-json", key, value)

      println(s"key: $key, value: $value")

      try {
        producer.send(record)
      } catch {
        case e: Throwable => e.printStackTrace()
      }

      sleep(100)
    }

  }
}

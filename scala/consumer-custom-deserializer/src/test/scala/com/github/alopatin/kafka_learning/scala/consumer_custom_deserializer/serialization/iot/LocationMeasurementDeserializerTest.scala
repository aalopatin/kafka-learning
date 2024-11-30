package com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer.serialization.iot

import com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer.model.iot.LocationMeasurement
import org.scalatest.funsuite.AnyFunSuite

class LocationMeasurementDeserializerTest extends AnyFunSuite{
  private val topic = "topic"
  private val outputExpected = LocationMeasurement("room 1", 23.0)
  private val input: Array[Byte] =  Array(0, 0, 0, 6, 114, 111, 111, 109, 32, 49, 64, 55, 0, 0, 0, 0, 0, 0)

  test("Method deserialize should return correct LocationMeasurement") {
    val serializer = new LocationMeasurementDeserializer()
    val outputResult = serializer.deserialize(topic, input)
    assert(outputExpected == outputResult)
  }
}

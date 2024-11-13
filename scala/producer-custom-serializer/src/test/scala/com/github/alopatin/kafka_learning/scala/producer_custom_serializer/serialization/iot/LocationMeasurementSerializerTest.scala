package com.github.alopatin.kafka_learning.scala.producer_custom_serializer.serialization.iot

import com.github.alopatin.kafka_learning.scala.producer_custom_serializer.model.iot.LocationMeasurement
import com.github.alopatin.kafka_learning.scala.producer_custom_serializer.serialization.iot.LocationMeasurementSerializer
import org.scalatest.funsuite.AnyFunSuite

class LocationMeasurementSerializerTest extends AnyFunSuite{
  private val topic = "topic"
  private val input = LocationMeasurement("room 1", 23.0)
  private val outputExpected: Array[Byte] =  Array(0, 0, 0, 6, 114, 111, 111, 109, 32, 49, 64, 55, 0, 0, 0, 0, 0, 0)

  test("Method serialize should return correct Array[Byte]") {
    val serializer = new LocationMeasurementSerializer()
    val outputResult = serializer.serialize(topic, input)
    assert(outputExpected sameElements outputResult)
  }
}

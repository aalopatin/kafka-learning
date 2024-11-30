package com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer.serialization.iot

import com.github.alopatin.kafka_learning.scala.consumer_custom_deserializer.model.iot.LocationMeasurement
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import java.nio.ByteBuffer

class LocationMeasurementDeserializer extends Deserializer[LocationMeasurement]{
  override def deserialize(topic: String, data: Array[Byte]): LocationMeasurement = {
    try {
      val buffer = ByteBuffer.wrap(data)
      val locationLength = buffer.getInt()

      val locationBytes = new Array[Byte](locationLength)
      buffer.get(locationBytes)

      val location = new String(locationBytes)

      val measurement = buffer.getDouble

      LocationMeasurement(location, measurement)
    } catch {
      case e:Throwable => throw new SerializationException("Error when deserializing Array[Byte] to LocationMeasurement", e)
    }
  }
}

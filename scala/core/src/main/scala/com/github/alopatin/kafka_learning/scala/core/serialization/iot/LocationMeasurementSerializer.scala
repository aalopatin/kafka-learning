package com.github.alopatin.kafka_learning.scala.core.serialization.iot

import com.github.alopatin.kafka_learning.scala.core.model.iot.LocationMeasurement
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import java.nio.ByteBuffer

class LocationMeasurementSerializer extends Serializer[LocationMeasurement]{
  override def serialize(topic: String, data: LocationMeasurement): Array[Byte] = {
    try {
      if (data == null)
        null
      else {
        val (serializedLocation, serializedLocationLength) = if (data.location != null) {
          val serializedLocation = data.location.getBytes("UTF-8")
          val serializedLocationLength = serializedLocation.length
          (serializedLocation, serializedLocationLength)
        } else {
          (Array[Byte](), 0)
        }

        val buffer = ByteBuffer.allocate(4 + serializedLocationLength + 8)
        buffer.putInt(serializedLocationLength)
        buffer.put(serializedLocation)
        buffer.putDouble(data.measurement)

        println(buffer.toString)
        buffer.array()
      }
    } catch {
      case e:Throwable => throw new SerializationException("Error when serializing LocationMeasurement to Array[Byte]", e)
    }
  }
}

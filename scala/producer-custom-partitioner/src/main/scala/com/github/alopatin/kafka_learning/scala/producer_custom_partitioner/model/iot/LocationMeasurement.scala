package com.github.alopatin.kafka_learning.scala.producer_custom_partitioner.model.iot

import com.fasterxml.jackson.annotation.JsonProperty


class LocationMeasurement(
                           _location: String,
                           _measurement: Double
                         ) {
  @JsonProperty("location")
  var location: String = _location

  @JsonProperty("measurement")
  var measurement: Double = _measurement

  override def toString = s"LocationMeasurement($location, $measurement)"
}

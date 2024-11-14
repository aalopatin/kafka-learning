package com.github.alopatin.kafka_learning.scala.producer_json.model.iot

import com.fasterxml.jackson.annotation.JsonProperty


class LocationMeasurementJson(
                               _location: String,
                               _measurement: Double
                                  ) {
  @JsonProperty("location")
  var location: String = _location

  @JsonProperty("measurement")
  var measurement: Double = _measurement
}

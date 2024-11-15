package com.github.alopatin.kafka_learning.scala.producer_json_case_class.model.iot

import com.fasterxml.jackson.annotation.JsonProperty


case class LocationMeasurement(
                                @JsonProperty("location") var location: String,
                                @JsonProperty("measurement") var measurement: Double
                              )

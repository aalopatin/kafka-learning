package com.github.alopatin.kafka_learning.scala.producer_json_case_class.model.iot

import com.fasterxml.jackson.annotation.JsonProperty


case class LocationMeasurementJson(
                                    @JsonProperty("location") location: String,
                                    @JsonProperty("measurement") measurement: Double
                                  )

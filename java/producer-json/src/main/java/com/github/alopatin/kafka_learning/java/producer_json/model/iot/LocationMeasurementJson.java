package com.github.alopatin.kafka_learning.java.producer_json.model.iot;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LocationMeasurementJson {
    @JsonProperty
    String location;

    @JsonProperty
    double measurement;

    public LocationMeasurementJson(String location, double measurement) {
        this.location = location;
        this.measurement = measurement;
    }

    @Override
    public String toString() {
        return "LocationMeasurementJson{" + "location='" + location + "'" + ", measurement=" + measurement + "}";
    }
}

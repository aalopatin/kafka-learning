package com.github.alopatin.kafka_learning.scala.producer_json_case_class.serialization

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer

import java.util

class CustomKafkaJsonSchemaSerializer[T] extends KafkaJsonSchemaSerializer[T] {

  override def configure(config: util.Map[String, _], isKey: Boolean): Unit = {
    super.configure(config, isKey)
    this.objectMapper.registerModule(DefaultScalaModule)
  }
}

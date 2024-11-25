package com.github.alopatin.kafka_learning.scala.producer_custom_partitioner.partitioner

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import java.util
import scala.util.matching.Regex

import scala.reflect.runtime.universe

class ValueFieldPartitioner extends Partitioner {

  private var pattern: Regex = _
  private var valueField: String = _

  override def configure(configs: util.Map[String, _]): Unit = {
    val patternString = configs.get("value.field.partitioner.pattern").toString
    pattern = new Regex(patternString)

    valueField = configs.get("value.field.partitioner.value.field").toString
  }

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val partitions = cluster.partitionsForTopic(topic)
    val numPartitions = partitions.size()

    // Dynamically determine the class of `value`
    val runtimeMirror = universe.runtimeMirror(value.getClass.getClassLoader)
    val classSymbol = runtimeMirror.classSymbol(value.getClass)
    val instanceMirror = runtimeMirror.reflect(value)

    // Access the field dynamically using reflection
    val fieldSymbol = classSymbol.toType.decl(universe.TermName(valueField)).asTerm
    val fieldMirror = instanceMirror.reflectField(fieldSymbol)
    val fieldValue = fieldMirror.get.toString

    val number = pattern.findFirstMatchIn(fieldValue) match {
      case Some(matched) => matched.group(1).toInt
      case None => 0
    }

    if (number != 0) {
      (number - 1) % (numPartitions - 1) + 1
    } else {
      0
    }

  }

  override def close(): Unit = {}

}

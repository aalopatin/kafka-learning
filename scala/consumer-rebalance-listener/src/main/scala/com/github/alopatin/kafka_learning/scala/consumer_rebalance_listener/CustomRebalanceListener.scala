package com.github.alopatin.kafka_learning.scala.consumer_rebalance_listener

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import java.util

class CustomRebalanceListener[K, V](val consumer: KafkaConsumer[K, V], val currentOffsets: util.Map[TopicPartition, OffsetAndMetadata]) extends ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        println("CustomRebalanceListener")
        println(currentOffsets)
        consumer.commitSync(currentOffsets)
        currentOffsets.clear()
      }
    }
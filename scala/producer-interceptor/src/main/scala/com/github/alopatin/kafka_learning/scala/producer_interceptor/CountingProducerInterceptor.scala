package com.github.alopatin.kafka_learning.scala.producer_interceptor

import com.github.alopatin.kafka_learning.scala.producer_interceptor.CountingProducerInterceptor.{numberSent, startTime}
import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

import java.nio.charset.StandardCharsets
import java.time.{LocalDate, LocalDateTime}
import java.util
import java.util.concurrent.atomic.AtomicLong

class CountingProducerInterceptor[K, V] extends ProducerInterceptor[K, V] {

  override def configure(configs: util.Map[String, _]): Unit = {}

   def onSend(record: ProducerRecord[K, V]): ProducerRecord[K, V] = {
     record.headers().add("start_time", startTime.toString.getBytes(StandardCharsets.UTF_8))
     record.headers().add("number", numberSent.incrementAndGet().toString.getBytes(StandardCharsets.UTF_8))
     record
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {}

  override def close(): Unit = {}

}

object CountingProducerInterceptor {
  val startTime: LocalDateTime = LocalDateTime.now()
  val numberSent: AtomicLong = new AtomicLong(0)
}

package com.github.alopatin.kafka_learning.producer_async

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class ProducerCallback extends Callback{
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null)
      exception.printStackTrace()
  }
}

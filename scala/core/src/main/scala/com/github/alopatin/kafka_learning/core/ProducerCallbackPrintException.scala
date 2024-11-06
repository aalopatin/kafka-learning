package com.github.alopatin.kafka_learning.core

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class ProducerCallbackPrintException extends Callback{
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null)
      exception.printStackTrace()
  }
}

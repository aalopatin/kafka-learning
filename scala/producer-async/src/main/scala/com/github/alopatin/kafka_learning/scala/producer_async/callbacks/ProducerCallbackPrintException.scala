package com.github.alopatin.kafka_learning.scala.producer_async.callbacks

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class ProducerCallbackPrintException extends Callback{
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null)
      exception.printStackTrace()
  }
}

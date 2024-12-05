package com.github.alopatin.kafka_learning.scala.consumer_custom_assignor

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.common.Cluster

class CustomAssignor extends ConsumerPartitionAssignor {


  override def assign(metadata: Cluster, groupSubscription: ConsumerPartitionAssignor.GroupSubscription): ConsumerPartitionAssignor.GroupAssignment = {
    new ConsumerPartitionAssignor.GroupAssignment()
  }

  override def name(): String = ROUNDROBIN_ASSIGNOR_NAME
}

object CustomAssignor {
  val ROUNDROBIN_ASSIGNOR_NAME: String = "roundrobin";
}

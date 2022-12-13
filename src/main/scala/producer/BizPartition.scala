package hst.peter.kafka
package producer

import org.apache.kafka.clients.producer.{Partitioner, RoundRobinPartitioner}
import org.apache.kafka.common.Cluster

class BizPartition extends RoundRobinPartitioner :

  // The algorithm logic of calculating partitions according to the key
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int =
    val n = key.toString.dropWhile(!_.isDigit).toInt
    println(s"number:$n, assigned partition: ${n % 3}")
    n % 3 // Here is a demo algorithm
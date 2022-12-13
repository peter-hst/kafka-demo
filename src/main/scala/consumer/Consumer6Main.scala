package hst.peter.kafka
package consumer

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

object Consumer6Main extends App :
  consumerWithBizConfirmCommitOffsetByOffset()

  // Consume by specified offset
  def consumerWithBizConfirmCommitOffsetByOffset() =
    val c = consumerClient("consumer2.properties") // Manually submit offset configuration
    val topics = List("t1-topic")

    val p0 = TopicPartition(topics.head, 0)
    c.assign(List(p0).asJavaCollection)

    // specified offset
    val seek = 500
    c.seek(p0, seek)
    loopForever(c)

    @tailrec
    def loopForever(c: KafkaConsumer[String, String]): Unit =
      val records = c.poll(Duration.ofSeconds(1))
      if !records.isEmpty then
        println(records.asScala.size.toString.padToCenter("-", 100))
        for
          tp <- records.partitions().asScala
          r <- records.records(tp).asScala
        do
          val msg = s"topic:${r.topic()}, partition:${r.partition()}, offset:${r.offset()}, ts:${r.timestamp()}, key:${r.key()} -> ${r.value()}"
          println(msg) // Business processing

          // commit offset by per partition
          println("After the business processing is completed, submit the offset")
          // records.records(tp).asScala.last.offset() + 1  // commit in batches by partition
          val offset = Map(tp -> OffsetAndMetadata(r.offset() + 1))
          //          c.commitSync(offset.asJava) // sync way is blocking....

          // records.records(tp).asScala.last.offset() + 1  // commit in batches by partition with async callback (High performance)
          c.commitAsync(offset.asJava, (offsets: util.Map[TopicPartition, OffsetAndMetadata], e: Exception) =>
            offsets.asScala.map(t => s"CB -> topic:${t._1.topic()}, partition:${t._1.partition()}, offset:${t._2.offset()}")
          )
      loopForever(c)

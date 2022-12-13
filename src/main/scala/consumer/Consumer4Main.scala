package hst.peter.kafka
package consumer

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}

import java.time.Duration
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

object Consumer4Main extends App :
  consumerWithBizForgotCommitOffsetByPartition()

  // forget submit offset, you can see receive messages repeatedly
  def consumerWithBizForgotCommitOffsetByPartition() =
    val c = consumerClient("consumer2.properties") // Manually submit offset configuration
    val topics = List("t1-topic", "t2-topic")
    //  c.subscribe("t\\d-topic".r.pattern)
    c.subscribe(topics.asJava)
    loopForever(c)

    @tailrec
    def loopForever(c: KafkaConsumer[String, String]): Unit =
      val records = c.poll(Duration.ofSeconds(1))
      if !records.isEmpty then
        println(records.asScala.size.toString.padToCenter("-", 100))
        // It does not submit offset
        records.partitions().asScala
          .flatMap(p => records.records(p).asScala)
          .map(r => s"topic:${r.topic()}, partition:${r.partition()}, offset:${r.offset()}, ts:${r.timestamp()}, key:${r.key()} -> ${r.value()}")
          .foreach(println)
      loopForever(c)
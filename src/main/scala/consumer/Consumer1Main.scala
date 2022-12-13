package hst.peter.kafka
package consumer

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

object Consumer1Main extends App :
  consumerWithAutoCommitOffset()

  // Auto submit offset
  def consumerWithAutoCommitOffset() =
    val c = consumerClient("consumer1.properties") // auto commit offset configuration
    val topics = "t1-topic" :: "t2-topic" :: Nil
    //  c.subscribe("t\\d-topic".r.pattern)
    c.subscribe(topics.asJava)
    loopForever(c, 0)

    @tailrec
    def loopForever(c: KafkaConsumer[String, String], total: Int): Unit =
      val records = c.poll(Duration.ofSeconds(1)).asScala
      val count = total + records.size
      if records.nonEmpty then
        println(records.size.toString.padToCenter("-", 100))
        records.map(r => s"topic:${r.topic()}, partition:${r.partition()}, offset:${r.offset()}, ts:${r.timestamp()}, key:${r.key()} -> ${r.value()}").foreach(println)
        println(s"current total: $total".padToCenter("=", 100))
      loopForever(c, count)

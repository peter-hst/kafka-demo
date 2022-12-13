package hst.peter.kafka
package consumer

import java.time.Duration
import scala.jdk.CollectionConverters.*

object Consumer2Main extends App :
  consumerWithBizConfirmCommitOffset()

  // Manually submit offset
  def consumerWithBizConfirmCommitOffset() =
    val c = consumerClient("consumer2.properties") // Manually submit offset configuration
    val topics = List("t1-topic", "t2-topic")
    //  c.subscribe("t\\d-topic".r.pattern)
    c.subscribe(topics.asJava)
    var total = 0
    while
      true
    do
      val records = c.poll(Duration.ofSeconds(1)).asScala
      if records.nonEmpty then
        println(records.size.toString.padToCenter("-", 100))
        records.map(r => s"topic:${r.topic()}, partition:${r.partition()}, offset:${r.offset()}, ts:${r.timestamp()}, key:${r.key()} -> ${r.value()}").foreach(println)
        val bizConsumerSucceedFlag = true

        if bizConsumerSucceedFlag then
          c.commitSync()
          total += records.size
          println("After the business processing is completed, submit the offset")
          println(s"current total: $total".padToCenter("=", 100))
        else
          println(s"Business processing exception, do not submit offset")
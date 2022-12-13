package hst.peter.kafka
package stream

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

object SourceProducerMain extends App :
  val topic = "t1-topic"
  produceMsgToTopic(topic)

  // Produce messages to the queue
  def produceMsgToTopic(topic: String) =
    val p = producerClient("producer2.properties")
    val file = getClass.getClassLoader.getResource("bible.txt").getFile
    val texts = readFile(file).getOrElse(Nil).filter(s => s.trim.nonEmpty)
    loopSimulateProductionMsg(p, texts)

    @tailrec
    def loopSimulateProductionMsg(p: KafkaProducer[String, String], texts: List[String]): Unit =
      if texts.isEmpty then
        println(" it's done ".padToCenter("=", 64))
        p.close()
      else
        val time = Random.nextInt(4000) + 200
        Thread.sleep(time)
        val qty = Math.min(Random.nextInt(25) + 5, texts.size) // random range (5-30)
        println(s"Ready to send $qty messages ${time}ms delay".padToCenter("=", 32))
        val messages = texts.take(qty).map(s => ProducerRecord(topic, s"key-${Random.nextInt(10001)}", s))
        messages.foreach { msg =>
          p.send(msg, // send msg (asynchronous nonblocking with callback)
            (r: RecordMetadata, e: Exception) => println(s"partition: ${r.partition()}, offset: ${r.offset()}, key: ${msg.key()} -> ${msg.value()}") // callback Anonymous implementation class
          )
        }
        loopSimulateProductionMsg(p, texts.drop(qty))
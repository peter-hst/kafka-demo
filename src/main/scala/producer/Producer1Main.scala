package hst.peter.kafka
package producer

import org.apache.kafka.clients.producer.ProducerRecord

import java.time.Duration

object Producer1Main extends App :
  sendMessagesAsyncBlocking()

  // asynchronous blocking (Not recommended!!)
  def sendMessagesAsyncBlocking() =
    val topic = "t1-topic"
    val p = producerClient("producer1.properties")
    // yield 100 messages
    val messages = 1 to 100 map { i => ProducerRecord(topic, s"k-$i", randStr(4, 9)) }
    println(s"producer -> $topic".padToCenter("-", 64))
    messages.foreach { msg =>
      val meta = p.send(msg).get() // send msg (asynchronous blocking)
      println(s"partition: ${meta.partition()}, offset: ${meta.offset()}, key: ${msg.key()} -> ${msg.value()}")
    }
    p.close(Duration.ofSeconds(3))
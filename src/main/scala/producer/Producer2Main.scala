package hst.peter.kafka
package producer

import org.apache.kafka.clients.producer.ProducerRecord

import java.time.Duration

object Producer2Main extends App :
  sendMessagesAsync() // asynchronous nonblocking

  // asynchronous nonblocking
  def sendMessagesAsync() =
    val topic = "t1-topic"
    val p = producerClient("producer1.properties")
    // yield 100 messages
    val messages = 1 to 100 map { i => ProducerRecord(topic, s"k-$i", randStr(4, 9)) }
    println(s"producer -> $topic".padToCenter("-", 64))
    messages.foreach { msg =>
      p.send(msg) // send msg (asynchronous nonblocking)
    }
    p.close()

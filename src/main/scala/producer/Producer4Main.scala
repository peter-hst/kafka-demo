package hst.peter.kafka
package producer

import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import java.time.Duration

object Producer4Main extends App :
  sendMessagesAsyncWithCallbackAndPartition() // asynchronous nonblocking with callback

  // asynchronous nonblocking with callback
  def sendMessagesAsyncWithCallbackAndPartition() =
    val topic = "t1-topic"
    val p = producerClient("producer2.properties") // important
    // yield 100 messages
    val messages = 1 to 100 map { i => ProducerRecord(topic, s"k-$i", randStr(4, 9)) }
    println(s"producer -> $topic".padToCenter("-", 64))
    messages.foreach { msg =>
      p.send(msg, // send msg (asynchronous nonblocking with callback)
        (r: RecordMetadata, e: Exception) => println(s"partition: ${r.partition()}, offset: ${r.offset()}, key: ${msg.key()} -> ${msg.value()}") // callback Anonymous implementation class
      )
    }
    p.close()


package hst.peter.kafka
package stream

import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}

import scala.jdk.CollectionConverters.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KTable, Produced}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig

object Stream1Main extends App :
  val topics = List("t1-topic", "t2-topic")
  val props = loadPropFile("stream1.properties")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass())
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass())
  val builder = StreamsBuilder()
  // orchestration workflow for streams builder
  // foreachStream(builder, topics.head)
  wordCountStream(builder, topics.head, topics.last)

  // start stream
  val streams = KafkaStreams(builder.build(), props)
  streams.start()


  // some stream function of demo
  def foreachStream(builder: StreamsBuilder, topic: String) =
    val source = builder.stream[String, String](topic)
    source.flatMapValues(v => v.replaceAll("[.?,:;]", " ").toLowerCase.split("\\s+").toList.asJava).foreach((k, v) => println(s"$k -> $v"))

  // some stream function of demo
  def wordCountStream(builder: StreamsBuilder, topicIn: String, topicOut: String) =
    val source = builder.stream[String, String](topicIn)
    val count = source
      .flatMapValues(v => v.toLowerCase.split("\\s+").toList.asJavaCollection)
      .groupBy((_, v:String) => v)
      .count()
    count.toStream.to(topicOut, Produced.`with`(Serdes.String(), Serdes.Long()))



package hst.peter

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.config.ConfigDef.Width

import java.io.File
import java.util.Properties
import util.{Failure, Random, Success}

package object kafka:

  val latters = (('a' to 'z') ++ ('A' to 'Z')).mkString
  val len = latters.length

  def randStr(max: Int, min: Int = 1) =
    val t = Math.max(max, min)
    val b = Math.min(max, min)
    (1 to Random.nextInt(t - b + 1) + b).map(_ => latters(Random.nextInt(len))).mkString

  // load a properties file
  def loadPropFile(name: String): Properties =
    val p = Properties()
    p.load(getClass.getClassLoader.getResourceAsStream(name))
    p

  // get a kafka adminClient instance
  def adminClient(p: Properties) = AdminClient.create(p)

  // build a producer with properties file
  def producerClient(propFile: String) = KafkaProducer[String, String](loadPropFile(propFile))

  // build a consumer with properties file
  def consumerClient(propFile: String) = KafkaConsumer[String, String](loadPropFile(propFile))

  extension (s: String)
    def padToCenter(symbol: String, width: Int): String =
      val half = width / 2
      symbol * half + s + symbol * half

  // read file
  def readFile(name: String) =
    try
      Success(io.Source.fromFile(name).getLines().toList)
    catch
      case e: Throwable => Failure(e)
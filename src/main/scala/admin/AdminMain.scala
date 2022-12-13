package hst.peter.kafka
package admin

import hst.peter.kafka.adminClient

import scala.jdk.CollectionConverters.*
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsOptions, ListTopicsOptions, NewTopic}
import org.apache.kafka.common.requests.CreateTopicsRequest

import java.io.FileReader
import java.util.Properties

object AdminMain extends App :

  // create topic list
  val topics = List("t1-topic", "t2-topic")
  val p = "admin.properties"
  //  val rs = createTopics(p, topics, 3, 2)
  //  println(rs.values().asScala.map((k, f) => s"$k -> ${f}"))


  // list topics
  val topicList = listTopics(adminClient(loadPropFile(p)))
  topicList.listings().get().asScala.map(x => s"topic-name: ${x.name()}, isInternal:${x.isInternal}, topic-id:${x.topicId()}").foreach(println)

  // delete topics
//  deleteTopics(adminClient(loadPropFile(p)), topics)

  /**
   * create topics
   *
   * @param propFile
   * @param topics
   * @param partitionsCount
   * @param rsCount
   * @return
   */
  def createTopics(propFile: String, topics: List[String], partitionsCount: Int = 1, rsCount: Int = 1) =
    val admin = adminClient(loadPropFile(propFile))
    admin.createTopics(topics.map(t => new NewTopic(t, partitionsCount, rsCount.toShort)).asJavaCollection)

  /**
   * list topics
   *
   * @param adminClient
   * @return
   */
  def listTopics(adminClient: AdminClient) = adminClient.listTopics(ListTopicsOptions().listInternal(true))

  /**
   * delete topics
   *
   * @param adminClient
   * @param topics
   * @return
   */
  def deleteTopics(adminClient: AdminClient, topics: List[String]) = adminClient.deleteTopics(topics.asJavaCollection)
package shared.connector

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink._
import scala.collection.JavaConverters._

object Connector {
  def sinkConnector = new SinkConnector {
    override def start(props: util.Map[String, String]): Unit = {}
    override def taskClass(): Class[_ <: Task]                = classOf[MySinkTask]
    override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] =
      List(Map.empty[String, String].asJava).asJava
    override def stop(): Unit        = {}
    override def config(): ConfigDef = new ConfigDef()
    override def version(): String   = "0.0.1"
  }

  class MySinkTask extends SinkTask {
    override def start(props: util.Map[String, String]): Unit = {}
    override def put(records: util.Collection[SinkRecord]): Unit = {
      val recordsList = records.asScala.toList
      val h           = recordsList.head
      val valueString = h.value().toString
      println(valueString)
    }
    override def stop(): Unit      = {}
    override def version(): String = "0.0.1"
  }
}

package eu.fasten.monitor.operators

import eu.fasten.monitor.JobConfig
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

case class TopicSummary(timestamp: Long,
                        key: String,
                        totalRecords: Long,
                        uniqueRecords: Long)
class ProcessRecord(jobConfig: JobConfig)
    extends ProcessFunction[ObjectNode, TopicSummary] {

  val keySeparator: String = ":"

  override def processElement(
      value: ObjectNode,
      ctx: ProcessFunction[ObjectNode, TopicSummary]#Context,
      out: Collector[TopicSummary]): Unit = {
    val key = getKeyFromTopic(jobConfig.key.toList, value)

  }

  def getKeyFromTopic(topicKeys: List[String], value: ObjectNode): String = {
    val keyValues = topicKeys
      .map(key => "/value/" + key.split("\\.").mkString("/"))
      .map(value.at(_).asText())

    keyValues.mkString(keySeparator)
  }
}

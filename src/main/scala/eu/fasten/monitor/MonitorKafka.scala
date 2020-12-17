package eu.fasten.monitor

import java.util.Properties

import eu.fasten.monitor.util.eu.fasten.synchronization.util.SimpleKafkaDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import scopt.OParser
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{
  GlobalWindows,
  TumblingProcessingTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

case class JobConfig(
    brokers: Seq[String] = Seq(),
    topic: String = "",
    key: Seq[String] = Seq(),
    emitTime: Long = 1,
    influxHost: String = "",
    influxPort: Int = 0,
    influxDatabase: String = "kafka-monitor",
    parallelism: Int = 1,
    backendFolder: String = "/mnt/fasten/flink-kafka-monitor/"
)
object MonitorKafka {

  val configBuilder = OParser.builder[JobConfig]
  val configParser = {
    import configBuilder._
    OParser.sequence(
      programName("MonitorKafka"),
      head("Monitor Kafka Topics to InfluxDB"),
      opt[Seq[String]]('b', "brokers")
        .required()
        .valueName("<broker1>,<broker2>,...")
        .action((x, c) => c.copy(brokers = x))
        .text("A set of Kafka brokers to connect to."),
      opt[String]('t', "topic")
        .required()
        .valueName("<topic>")
        .action((x, c) => c.copy(topic = x))
        .text("The topic to read from."),
      opt[Seq[String]]('k', "key")
        .required()
        .valueName("<key1>,<key2>,...")
        .action((x, c) => c.copy(key = x))
        .text("The keys to aggregate on."),
      opt[Long]('t', "emit_time")
        .required()
        .valueName("<time>")
        .action((x, c) => c.copy(emitTime = x))
        .text("The time (in seconds) to emit statistics to InfluxDB."),
      opt[String]("influx_host")
        .required()
        .valueName("<host>")
        .action((x, c) => c.copy(influxHost = x))
        .text("InfluxDB hostname."),
      opt[Int]("influx_port")
        .required()
        .valueName("<port>")
        .action((x, c) => c.copy(influxPort = x))
        .text("InfluxDB port."),
      opt[String]("influx_database")
        .required()
        .valueName("<database>")
        .action((x, c) => c.copy(influxDatabase = x))
        .text("InfluxDB database."),
      opt[Int]("parallelism")
        .optional()
        .text("The amount of parallel workers for Flink.")
        .action((x, c) => c.copy(parallelism = x)),
      opt[String]("backendFolder")
        .optional()
        .text("Folder to store checkpoint data of Flink.")
        .action((x, c) => c.copy(backendFolder = x)),
    )
  }
  val streamEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  val keySeparator: String = ":"

  def main(args: Array[String]) = {
    val jobConfig = OParser.parse(configParser, args, JobConfig())

    streamEnv
      .addSource(setupConsumer(jobConfig.get))
      .map(x => (getKeyFromTopic(jobConfig.get.key.toList, x), 1)) // Map to (key, 1)
      .keyBy(_._1) // Key by that key
      .sum(1) // Rolling aggregate.
      .uid("sum-per-key")
      .name("Sum per key")
      .map { record =>
        if (record._2 > 1) {
          ("non_unique", 1)
        } else {
          ("unique", 1)
        }
      } // Then for each summed key, transform it to ("unique", 1) and ("non_unique", sum).
      .keyBy(_._1) // Key by either unique or non_unique.
      .window(TumblingProcessingTimeWindows.of(
        Time.seconds(jobConfig.get.emitTime))) // Tumbling window of x seconds.
      .sum(1)
      .uid("sum-per-tumbling-window")
      .name("sum-per-tumbling-window")
      .print()

  }

  def getKeyFromTopic(topicKeys: List[String], value: ObjectNode): String = {
    val keyValues = topicKeys
      .map(key => "/value/" + key.split("\\.").mkString("/"))
      .map(value.at(_).asText())

    keyValues.mkString(keySeparator)
  }

  def setupConsumer(c: JobConfig): FlinkKafkaConsumer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", c.brokers.mkString(","))
    properties.setProperty("group.id", f"fasten.${c.topic}.monitor")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("max.partition.fetch.bytes", "50000000")
    properties.setProperty("message.max.bytes", "50000000")

    val consumer: FlinkKafkaConsumer[ObjectNode] =
      new FlinkKafkaConsumer[ObjectNode](
        c.topic,
        new SimpleKafkaDeserializationSchema(true),
        properties)

    consumer
  }

}

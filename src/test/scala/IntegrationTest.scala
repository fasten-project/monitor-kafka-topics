import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import eu.fasten.monitor.MonitorKafka
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.funsuite.AnyFunSuite
import akka.actor.{Actor, ActorSystem, Props}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends AnyFunSuite with EmbeddedKafka {

  val mapper: ObjectMapper = new ObjectMapper()

  test ("Full integration test") {
    EmbeddedKafka.start()

    val system = ActorSystem.create("messages")

    system.scheduler.schedule(0 seconds, 2 seconds) {
      List.range(0, 50).map(x => getMessage(false, false)).foreach(x => publishStringMessageToKafka("test", x.toString))
      List.range(0, 25).map(x => getMessage(false, true)).foreach(x => publishStringMessageToKafka("test", x.toString))
      List.range(0, 50).map(x => getMessage(true, false)).foreach(x => publishStringMessageToKafka("test", x.toString))
    }

    //MonitorKafka.main(getStartString().split(" "))
    EmbeddedKafka.stop()
  }

  def getMessage(unique: Boolean, brokenKey: Boolean): ObjectNode = {
    val node = mapper.createObjectNode()

    if (unique) {
      node
        .putObject("key")
        .put("key1", UUID.randomUUID().toString)
        .put("key2", "wouter")
    } else if (brokenKey) {
      node
        .putObject("key2")
    } else {
      node
        .putObject("key")
        .put("key1", "wouter")
        .put("key2", "wouter")
    }

    node
  }

  def getStartString(): String = {
    "-b localhost:6001 -t test -k key.key1,key.key2 -t 5 --influx_host localhost --influx_port 8086 --influx_database fasten"
  }
}

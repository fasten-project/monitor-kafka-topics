import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.funsuite.AnyFunSuite

class IntegrationTest extends AnyFunSuite with EmbeddedKafka {

  val mapper: ObjectMapper = new ObjectMapper()

  def getMessage(unique: Boolean): ObjectNode = {
    val node = mapper.createObjectNode()

    if (unique) {
      node
        .putObject("key")
        .put("key1", UUID.randomUUID().toString)
        .put("key2", "wouter")
    } else {}

    node
  }

  def getStartString(): String = {
    return "-b localhost:6001"
  }
}

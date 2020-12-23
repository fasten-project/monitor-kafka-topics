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

  test("Full integration test") {
    EmbeddedKafka.start()

    val system = ActorSystem.create("messages")

    system.scheduler.schedule(0 seconds, 2 seconds) {
      publishStringMessageToKafka(
        "test",
        "{\"input\":{\"input\":{\"date\":1554435698,\"groupId\":\"systems.manifold\",\"artifactId\":\"manifold-ext-test\",\"version\":\"0.58-alpha\"},\"plugin_version\":\"0.1.2\",\"consumed_at\":1601297053,\"payload\":{\"date\":1554435698,\"repoUrl\":\"\",\"forge\":\"mvn\",\"groupId\":\"systems.manifold\",\"sourcesUrl\":\"http://172.16.45.117:8081/repository/maven-central/systems/manifold/manifold-ext-test/0.58-alpha/manifold-ext-test-0.58-alpha-sources.jar\",\"artifactId\":\"manifold-ext-test\",\"dependencyData\":{\"dependencyManagement\":{\"dependencies\":[]},\"dependencies\":[{\"versionConstraints\":[{\"isUpperHardRequirement\":false,\"isLowerHardRequirement\":false,\"upperBound\":\"0.58-alpha\",\"lowerBound\":\"0.58-alpha\"}],\"groupId\":\"systems.manifold\",\"scope\":\"\",\"classifier\":\"\",\"artifactId\":\"manifold-ext\",\"exclusions\":[],\"optional\":false,\"type\":\"\"},{\"versionConstraints\":[{\"isUpperHardRequirement\":false,\"isLowerHardRequirement\":false,\"upperBound\":\"0.58-alpha\",\"lowerBound\":\"0.58-alpha\"}],\"groupId\":\"systems.manifold\",\"scope\":\"\",\"classifier\":\"\",\"artifactId\":\"manifold-image\",\"exclusions\":[],\"optional\":false,\"type\":\"\"},{\"versionConstraints\":[{\"isUpperHardRequirement\":false,\"isLowerHardRequirement\":false,\"upperBound\":\"4.12\",\"lowerBound\":\"4.12\"}],\"groupId\":\"junit\",\"scope\":\"compile\",\"classifier\":\"\",\"artifactId\":\"junit\",\"exclusions\":[],\"optional\":false,\"type\":\"\"}]},\"projectName\":\"Manifold :: ExtensionTest\",\"version\":\"0.58-alpha\",\"commitTag\":\"\",\"packagingType\":\"jar\",\"parentCoordinate\":\"systems.manifold:manifold-deps-parent:0.58-alpha\"},\"host\":\"fasten-pom-analyzer-5bf88f9956-tbbb9\",\"created_at\":1601297053,\"plugin_name\":\"POMAnalyzer\"},\"plugin_version\":\"0.1.2\",\"consumed_at\":1601297231,\"payload\":{\"link\":\"http://lima.ewi.tudelft.nl/mvn/m/manifold-ext-test/manifold-ext-test_systems.manifold_0.58-alpha.json\",\"dir\":\"/mnt/fasten/mvn/m/manifold-ext-test/manifold-ext-test_systems.manifold_0.58-alpha.json\"},\"host\":\"fasten-server-opal-8558c94c47-s62hm\",\"created_at\":1601297231,\"plugin_name\":\"OPAL\"}"
      )
      // List.range(0, 50).map(x => getMessage(false, false)).foreach(x => publishStringMessageToKafka("test", x.toString))
      //List.range(0, 25).map(x => getMessage(false, true)).foreach(x => publishStringMessageToKafka("test", x.toString))
      //List.range(0, 50).map(x => getMessage(true, false)).foreach(x => publishStringMessageToKafka("test", x.toString))
    }

    MonitorKafka.main(getStartString().split(" "))
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
    "-b localhost:6001 -t test -k input.input.groupId,input.input.artifactId,input.input.version -t 5 --influx_host localhost --influx_port 8086 --influx_database fasten"
  }
}

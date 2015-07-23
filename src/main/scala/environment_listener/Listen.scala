package environment_listener

import org.openmole.core.batch.environment.BatchEnvironment
import org.openmole.core.event._

import java.util.concurrent.ConcurrentHashMap

object Hello {
  val data_store = new ConcurrentHashMap[BatchEnvironment, Any]()

  def monitor_env(env: BatchEnvironment) = {
    println("Ouhlala je vais regarder")
    env listen {
      case _ => println("Something is happening")
    }
  }

  def do_the_thing = {
    println("toto")
  }
}


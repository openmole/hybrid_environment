package environment_listener

import org.openmole.core.batch.environment.BatchEnvironment
import org.openmole.core.tools.service.Logger
import org.openmole.core.event._

class EnvListener(env : BatchEnvironment) extends Runnable{

    def run: Unit = {
        Listener.Log.logger.fine(s"Start to monitor the environment $env")

        env listen {
            case _ => println("I'm hearing thingsssss")
        }
    }
}

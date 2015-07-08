package environment_listener

import org.openmole.core.batch.environment.BatchEnvironment
import org.openmole.core.batch.environment.BatchEnvironment.{BeginUpload, EndUpload}
import org.openmole.core.tools.service.Logger
import org.openmole.core.event._
import org.openmole.core.event.Event
import org.openmole.core.workflow.mole.MoleExecution.JobStatusChanged

import scala.collection.mutable.HashMap

class EnvListener(env : BatchEnvironment) extends Runnable{
    val transferStarts = new HashMap[Long, Long]

    def run: Unit = {
        Listener.Log.logger.info(s"Start to monitor the environment $env")

        env listen {
            case (_ , BeginUpload(id, path, stor)): (Any, Event[BatchEnvironment])) => {
//                println(s"Begin: $path $stor")
                transferStarts.put(id, System.currentTimeMillis())
            }
            case (_, EndUpload(id, path, stor)) => {
//                println(s"Begin: $path $stor")
                val v = System.currentTimeMillis() - transferStarts(id)
                Listener.put_value(env, id, "upload_time", v)
            }
            case _ => println("I'm hearing thingsssss")
        }
    }
}

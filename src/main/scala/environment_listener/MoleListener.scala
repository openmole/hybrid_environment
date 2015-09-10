package environment_listener

import scala.collection.mutable

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.mole.{ Capsule, MoleExecution }
import org.openmole.core.workflow.mole.MoleExecution.JobCreated
import org.openmole.core.event._

import hybrid.HybridEnvironment

class MoleListener(me: MoleExecution, hyb: HybridEnvironment) extends Runnable {

    private val capsule_list = mutable.MutableList[Capsule]()

    def run(): Unit = {
        me listen {
            case (_, JobCreated(_, capsule)) =>
                this.synchronized {
                    if (!capsule_list.contains(capsule)) {
                        println("Found new capsule")
                        capsule_list += capsule
                        hyb.globalStrategy.save(Listener.exportCompletedJobs(),
                            Listener.env_list.toList.map(_.asInstanceOf[SimpleBatchEnvironment]))
                        Listener.flush_data_store()
                    }
                }
        }
    }
}

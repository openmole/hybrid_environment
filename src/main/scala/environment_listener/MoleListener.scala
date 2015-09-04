package environment_listener

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.mole.{ Capsule, MoleExecution }
import org.openmole.core.workflow.mole.MoleExecution.JobCreated
import org.openmole.core.event._

import scala.collection.mutable

class MoleListener(me: MoleExecution) extends Runnable {

    private val capsule_list = mutable.MutableList[Capsule]()

    def run(): Unit = {
        me listen {
            case (_, JobCreated(_, capsule)) =>
                this.synchronized {
                    if (!capsule_list.contains(capsule)) {
                        capsule_list += capsule
                        Listener.hyb.globalStrategy.save(Listener.exportCompletedJobs(),
                            Listener.env_list.toList.map(_.asInstanceOf[SimpleBatchEnvironment]))
                        Listener.flush_data_store()
                    }
                }
        }
    }
}

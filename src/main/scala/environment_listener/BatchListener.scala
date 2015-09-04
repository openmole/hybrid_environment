package environment_listener

import java.util.Calendar

import scala.collection.mutable

import org.openmole.core.event._

import org.openmole.core.batch.environment.BatchEnvironment
import org.openmole.core.batch.environment.BatchEnvironment.{ BeginUpload, EndUpload }
import org.openmole.core.workflow.execution.Environment

import org.openmole.core.workflow.job.Job

class BatchListener(env: BatchEnvironment) extends Runnable {
    private val timingList = new mutable.HashMap[Job, Long]()
    private val L = Listener.Log.logger
    private var n = 0

    /**
     * Start to monitor the environment
     */
    def run(): Unit = {
        env listen {
            case (_, BeginUpload(id, file, path, storage, job)) =>
                //                println(s"$n Begin upload on $job with id $id")
                n += 1
                Listener.createJobMap(job, env)
                timingList(job) = Calendar.getInstance().getTimeInMillis / 1000
            case (_, EndUpload(id, file, path, storage, job, exception)) =>
                //                println(s"Ended upload on $job with id $id")
                Listener.put(job,
                    env,
                    "uploadTime",
                    Calendar.getInstance().getTimeInMillis / 1000 - timingList(job))
                //                println(s"$job, $env")
                //                Listener.printJob((job, env))
                timingList.remove(job)
        }
    }
}
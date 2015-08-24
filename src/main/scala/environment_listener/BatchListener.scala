package environment_listener

import java.util.Calendar
import org.openmole.core.workflow.execution.ExecutionJob

import scala.collection.mutable

import org.openmole.core.event._

import org.openmole.core.batch.environment.{ BatchExecutionJob, BatchEnvironment }
import org.openmole.core.batch.environment.BatchEnvironment.{ BeginUpload, EndUpload }

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
                Listener.createJobMap(job)
                timingList(job) = Calendar.getInstance().getTimeInMillis
            case (_, EndUpload(id, file, path, storage, job, exception)) =>
                //                println(s"Ended upload on $job with id $id")
                Listener.put(job,
                    "uploadTime",
                    Calendar.getInstance().getTimeInMillis - timingList(job))
                timingList.remove(job)
        }
    }
}
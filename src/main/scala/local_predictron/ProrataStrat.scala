package local_predictron

import hybrid.HybridEnvironment._
import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

/**
 * Use the maximum time in a chunk, and divide it by the number of completed jobs.
 * It is mostly used to give an edge to heavily parallel environments.
 */
object ProrataStrat extends LocalStrategy {

    def subpredict(data: List[Map[String, Any]], env_l: List[SimpleBatchEnvironment]): (t_pred, Int) = {

        println("Predict prorata")
        val dl = data.filter(_("completed").asInstanceOf[Boolean])
        val maxTime = dl.map(_("totalTime").asInstanceOf[Long]).max
        println(s"maxTime: $maxTime")

        (env_l.map(e => (e, maxTime / dl.count(_("senv") == e).toDouble)), dl.size)
    }
}

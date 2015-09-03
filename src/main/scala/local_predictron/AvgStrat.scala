package local_predictron

import hybrid.HybridEnvironment._
import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

/**
 * Does an average on the values from the chunk. Only use data from completed jobs.
 */
object AvgStrat extends LocalStrategy {
    def subpredict(data: List[Map[String, Any]], env_l: List[SimpleBatchEnvironment]): (t_pred, Int) = {

        val dl: List[Map[String, Any]] = data.filter(_("completed").asInstanceOf[Boolean]).toList
        def calcAvg(env: SimpleBatchEnvironment) = {
            avg(dl.filter(m => m("senv") == env).map(m => m("execTime").asInstanceOf[Long]))
        }

        (env_l.map(e => (e, calcAvg(e))), dl.size)
    }

    /**
     * Compute the average value of the list
     * @param l The list
     * @return The average
     */
    private def avg(l: List[Long]): Double = {
        println(l.sum.toDouble)
        println(l.size)
        l.sum.toDouble / l.size
    }
}

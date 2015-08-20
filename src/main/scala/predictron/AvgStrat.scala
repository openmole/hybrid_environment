package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.job.Job

class AvgStrat extends PredictStrategy {

    def predict(data: Map[Job, Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)] = {
        val dl: List[Map[String, Any]] = data.values.toList
        def calcAvg(env: SimpleBatchEnvironment) = {
            avg(dl.filter(m => m("senv") == env).map(m => m("exec_time").asInstanceOf[Long]))
        }

        env_l.map(e => (e, calcAvg(e)))
    }

    /**
     * Compute the average value of the list
     * @param l The list
     * @return The average
     */
    private def avg(l: List[Long]): Double = {
        l.sum.toDouble / l.size
    }
}

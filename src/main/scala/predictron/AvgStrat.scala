package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.job.Job

class AvgStrat extends PredictStrategy {

    def predict(data: Map[Job, Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)] = {
        println("Predict")
        val dl: List[Map[String, Any]] = data.values.toList
        println(dl.size)
        def calcAvg(env: SimpleBatchEnvironment) = {
            println("AVG")
            println(dl.filter(m => m("senv") == env).size)
            avg(dl.filter(m => m("senv") == env).map(m => m("execTime").asInstanceOf[Long]))
        }

        env_l.map(e => (e, calcAvg(e)))
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

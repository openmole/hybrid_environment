package local_predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

object AvgStrat extends LocalStrategy {

    def predict(data: Map[(Job, Environment), Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)] = {

        println("Predict")
        val dl: List[Map[String, Any]] = data.values.filter(_("completed").asInstanceOf[Boolean]).toList
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

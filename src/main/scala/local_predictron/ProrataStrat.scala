package local_predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

object ProrataStrat extends LocalStrategy {

    def predict(data: Map[(Job, Environment), Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)] = {
        println("Predict prorata")
        val dl = data.values.filter(_("completed").asInstanceOf[Boolean])
        val maxTime = dl.map(_("totalTime").asInstanceOf[Long]).max
        println(s"maxTime: $maxTime")

        env_l.map(e => (e, maxTime / dl.count(_("senv") == e).toDouble))
    }
}

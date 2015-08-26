package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

class ProrataStrat extends PredictStrategy {

    def predict(data: Map[(Job, Environment), Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)] = {
        println("Predict prorata")
        val dl = data.values
        val maxTime = dl.map(_("totalTime").asInstanceOf[Long]).max
        println(s"maxTime: $maxTime")

        env_l.map(e => (e, maxTime / dl.count(_("senv") == e).toDouble))
    }
}

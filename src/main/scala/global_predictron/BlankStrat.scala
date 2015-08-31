package global_predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

object BlankStrat extends GlobalStrategy {

    def predict(data: Map[(Job, Environment), Map[String, Any]],
        env_l: List[SimpleBatchEnvironment],
        local_pred: List[(SimpleBatchEnvironment, Double)]): List[(SimpleBatchEnvironment, Double)] = {
        local_pred
    }
}

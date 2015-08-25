package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

abstract class PredictStrategy {
    def predict(data: Map[(Job, Environment), Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)]
}

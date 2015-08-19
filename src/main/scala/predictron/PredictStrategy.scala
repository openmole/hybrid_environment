package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.job.Job

abstract class PredictStrategy {
    def predict(data: Map[Job, Map[String, Any]]): Seq[(SimpleBatchEnvironment, Long)]
}
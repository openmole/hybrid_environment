package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.job.Job

class AvgStrat extends PredictStrategy {

    def predict(data: Map[Job, Map[String, Any]]): Seq[(SimpleBatchEnvironment, Long)] = {
        null
    }
}

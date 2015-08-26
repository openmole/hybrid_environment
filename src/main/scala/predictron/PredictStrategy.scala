package predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

abstract class PredictStrategy {
    /**
     * Will predict the execution time of the job for each environment
     * @param data The data_store of the completed jobs
     * @param env_l The list of environments
     * @return A list of environment with predicted time
     */
    def predict(data: Map[(Job, Environment), Map[String, Any]], env_l: List[SimpleBatchEnvironment]): List[(SimpleBatchEnvironment, Double)]
}

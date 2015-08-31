package global_predictron

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

abstract class GlobalStrategy {
    /**
     * Will predict the execution time of the job for each environment, using the predictions of the local strategy as well.
     * @param data The data_store of the completed jobs
     * @param env_l The list of environments
     * @param local_pred The prediction of the local strategy
     * @return A list of environment with predicted time
     */
    def predict(data: Map[(Job, Environment), Map[String, Any]],
        env_l: List[SimpleBatchEnvironment],
        local_pred: List[(SimpleBatchEnvironment, Double)]): List[(SimpleBatchEnvironment, Double)]
}

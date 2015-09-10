package local_predictron

import hybrid.HybridEnvironment.t_pred
import org.openmole.core.batch.environment.SimpleBatchEnvironment

abstract class LocalStrategy {
    /**
     * Will predict the execution time of the job for each environment
     *
     * The basic implementation will call subpredict on chunk0 and chunk -1 (if exists).
     * May be overriden in other strategies.
     * @param data The data, as a list of chunks
     *             A chunk is a list of data
     *             (chunk0, chunk-1, chunk-2, ...)
     * @param env_l The list of environments
     * @return (current_prediction, current_weight, previous_prediction, previous_weight)
     */
    def predict(data: List[List[Map[String, Any]]], env_l: List[SimpleBatchEnvironment]): (t_pred, Int, t_pred, Int) = {

        val (current_pred, current_weight): (t_pred, Int) =
            subpredict(data(0), env_l)

        if (data.size > 1) {
            val (previous_pred, previous_weight) = subpredict(data(1), env_l)

            (current_pred, current_weight, previous_pred, previous_weight)
        } else {
            (current_pred, current_weight, env_l.map((_, .0)), 0)
        }
    }

    /**
     * Take data from one chunk and predict on it
     * @param data The data
     * @param env_l Env list
     * @return Pair of (
     *             Prediction for this chunk only. Used at least for current_pred
     *             Weight of the prediction
     *         )
     */
    protected def subpredict(data: List[Map[String, Any]], env_l: List[SimpleBatchEnvironment]): (t_pred, Int)
}

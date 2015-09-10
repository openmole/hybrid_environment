package hybrid

import org.openmole.core.batch.environment.SimpleBatchEnvironment

object Merger {

    /**
     * Merge the data from the local and global prediction
     * Use the weight (number of datapoint used to predict) to balance the predictions.
     *
     * It is just an average between the two.
     * @param previous_pred Local predictions
     * @param previous_weight Weight of local predictions
     * @param current_pred Global predictions
     * @param current_weight Weight of global predictions
     * @return The average of all predictions.
     */
    def merge(previous_pred: List[(SimpleBatchEnvironment, Double)],
        previous_weight: Int,
        current_pred: List[(SimpleBatchEnvironment, Double)],
        current_weight: Int): List[(SimpleBatchEnvironment, Double)] = {

        // FIXME refactor head -> flatten
        val envsToPredictions = current_pred.groupBy(_._1).mapValues(_.head._2)

        val s = previous_weight + current_weight
        // merge previous and current predictions, by taking care of giving more weight to predictions with more datapoints
        previous_pred.map(d => (d._1, (d._2 * previous_weight + envsToPredictions(d._1) * current_weight) / s))
    }
}

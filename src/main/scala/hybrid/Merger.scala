package hybrid

import org.openmole.core.batch.environment.SimpleBatchEnvironment

object Merger {

    /**
     * Merge the data from the local and global prediction
     * Use the weight (number of datapoint used to predict) to balance the predictions.
     *
     * It is just an average between the two.
     * @param local_pred Local predictions
     * @param local_weight Weight of local predictions
     * @param global_pred Global predictions
     * @param global_weight Weight of global predictions
     * @return The average of all predictions.
     */
    def merge(local_pred: List[(SimpleBatchEnvironment, Double)],
        local_weight: Int,
        global_pred: List[(SimpleBatchEnvironment, Double)],
        global_weight: Int): List[(SimpleBatchEnvironment, Double)] = {

        val mg = global_pred.groupBy(_._1).mapValues(_.head._2)

        val s = local_weight + global_weight
        local_pred.map(d => (d._1, (d._2 * local_weight + mg(d._1) * global_weight) / s))
    }
}

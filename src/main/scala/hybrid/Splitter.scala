package hybrid

import java.text.SimpleDateFormat
import java.util.Calendar

import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job

object Splitter {
    /**
     * Current chunk according to tim. Chunk goes from 0 to 83. (2h)
     */
    var current_chunk: Int = 0

    /**
     * Split the data into a list of list: each submit contains data for a specific chunk
     * They are sorted by age: The first submit is the current chunk, the second one the chunk -1, etc...
     * If a chunk is missing (-1, -3, but no -2), will not have an empty list for it.
     *
     * Problems may arise if execution has been going on for more than a week and chunks are overlapping.
     * @param data The data to be splitted
     * @return (data chunk0, data chunk-1, data chunk-2, ...)
     */
    def split(data: Map[(Job, Environment), Map[String, Any]]): List[List[Map[String, Any]]] = {
        updateCurrentChunk()
        println(s"Current Chunk: $current_chunk")

        val mapchunks = data.values.groupBy(getChunk)
        mapchunks.foreach(x => println(s"${x._2}"))
        println("Sorted chunks")
        mapchunks.keys.toList.sortBy(cmpChunk).foreach(println)
        mapchunks.keys.toList.sortBy(cmpChunk).map(mapchunks(_).toList)
    }

    /**
     * Update the value of the current chunk.
     * Called at the beginning of split.
     * Ask the calendar what time it is, then update the current chunk
     */
    private def updateCurrentChunk() = {
        val t = Calendar.getInstance().getTime
        /* day of the week goes from 1 to 7 */
        val dw = new SimpleDateFormat("F").format(t).toInt
        val h = new SimpleDateFormat("HH").format(t).toInt
        current_chunk = h / 2 + (dw - 1) * 11
    }

    /**
     * Get the chunk of the given datapoint (of a completed job)
     * @param datapoint The datapoint
     * @return Its chunk
     */
    private def getChunk(datapoint: Map[String, Any]): Int = {
        (datapoint("day_w").asInstanceOf[String].toInt - 1) * 11
        +datapoint("hour").asInstanceOf[String].toInt / 2
    }

    /**
     * Will compare the given chunk to the current chunk, and compute the diffence/distance
     * in number of chunk
     *
     * Special case: current chunk has looped (since it goes from 0 to 83 to cover the whole week).
     * In this case, will add 84 to the current chunk. Therefore, we are still able to compute the distance.
     *
     * More work would be needed to actually have the real difference between two chunks.
     * @param c The chunk to compare
     * @return The difference between the two.
     *         Read the previous explanations.
     */
    private def cmpChunk(c: Int): Int = {
        if (current_chunk >= c) {
            current_chunk - c
        } else {
            current_chunk + 84 - c
        }
    }
}

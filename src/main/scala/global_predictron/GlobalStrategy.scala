package global_predictron

import scala.collection.mutable
import scala.io.Source
import java.io.{ FileWriter, File, BufferedWriter }

import org.openmole.plugin.environment.condor.CondorEnvironment
import org.openmole.plugin.environment.egi.EGIEnvironment
import org.openmole.plugin.environment.slurm.SLURMEnvironment
import org.openmole.plugin.environment.ssh.SSHEnvironment

import org.openmole.core.batch.environment.SimpleBatchEnvironment
import org.openmole.core.workflow.job.Job

import hybrid.Splitter.getChunk

import GlobalStrategy._

object GlobalStrategy {

    /**
     * @param e_type Type of environment
     * @param e_address Host machine / address
     */
    case class Env_key(e_type: String, e_address: String) {
        override def toString: String = {
            e_type ++ " " ++ e_address
        }
    }
    /**
     * @param ratio between current and next chunks
     * @param weight number of jobs (to even ratio)
     * */
    case class ChunkValue(ratio: Double, weight: Int) {
        override def toString: String = {
            ratio.toString ++ "," ++ weight.toString
        }
    }
}

abstract class GlobalStrategy {
    protected var path: String = null

    /** Map [
      *  key -> environment
      *  value -> list of values for each chunks for this environment
      */
    val knowledge = new mutable.HashMap[Env_key, Array[ChunkValue]]()

    /**
     * Will predict the execution time of the job for each environment, using the predictions of the local strategy as well.
     * @param env_l The list of environments
     * @param local_pred The prediction of the local strategy
     * @return A list of environment with predicted time
     */
    def predict(env_l: List[SimpleBatchEnvironment],
        local_pred: List[(SimpleBatchEnvironment, Double)]): List[(SimpleBatchEnvironment, Double)]

    /**
     * Load the data about environments.
     * @param p path to file where to find knowledge
     */
    def load(p: String) = {
        path = p

        try {
            for (line <- Source.fromFile(path).getLines()) {
                println(line)
                val (key, values) = line.split(" ").splitAt(2)
                load_environment(key(0), key(1), values)
            }
        } catch {
            case ex: Exception => println(s"Load error: no such file as $path")
        }

        knowledge.foreach(println)
    }

    /**
     * Create a new list (of knowledge) for the environment
     * @param env The environment to add
     */
    def add_environment(env: SimpleBatchEnvironment) {
        val key = envToKey(env)
        add_environment(key.e_type, key.e_address)
    }

    protected def add_environment(kind: String, address: String): Unit = {
        knowledge(Env_key(kind, address)) = Array.fill(83) { ChunkValue(-1.0, 0) }
    }

    /**
     * Load the environment: create array and fill it with given data
     * @param kind The kind of the environment
     * @param address The address of the environment
     * @param data The data of the environment
     */
    protected def load_environment(kind: String, address: String, data: Array[String]) {
        knowledge(Env_key(kind, address)) = data.map(_.split(","))
            .map(a => ChunkValue(a(0).toDouble, a(1).toInt))
    }

    /**
     * Generate the keys for an environment
     * @param env The environment
     * @return (Kind, address)
     */
    protected def envToKey(env: SimpleBatchEnvironment): Env_key = {
        val env_kind: String = env match {
            case _: SSHEnvironment => "SSHEnvironment"
            case _: CondorEnvironment => "CondorEnvironment"
            case _: SLURMEnvironment => "SLURMEnvironment"
            case _: EGIEnvironment => "EGIEnvironment"
            case _ =>
                println(s"Unsupported environment (envKind): $env.")
                ""
        }

        val address: String = env match {
            case e: SSHEnvironment => e.host
            case e: CondorEnvironment => e.host
            case e: SLURMEnvironment => e.host
            case e: EGIEnvironment => e.voName
            case _ =>
                println(s"Unsupported environment (envName): $env.")
                ""
        }

        Env_key(env_kind, address)
    }

    /**
     * Update and save the knowledge.
     * Use the previous set path variable (with function load)
     * @param data The data needed to update the knowledge
     *             Assume contains only completed jobs
     */
    def save(data: Map[(Job, String), Map[String, Any]], lenv: List[SimpleBatchEnvironment]) {
        println(s"Saving with ${data.size} samples.")

        // Update for each environment
        lenv.foreach(e =>
            update_env(envToKey(e), data.filter(_._2("senv").asInstanceOf[String] == e.toString)))

        /* Save */
        // (Re)create File
        val file = new File(path)
        val bw = new BufferedWriter(new FileWriter(file))

        // Loop to write line by line
        knowledge.foreach(writeKnowledge(bw, _))

        bw.flush()
        bw.close()
    }

    /**
     * Write the knowledge of the given environment
     * @param bw The writer where to write
     * @param k The knowledge to write
     */
    protected def writeKnowledge(bw: BufferedWriter, k: (Env_key, Array[ChunkValue])) {
        println(s"Writing knowledge about ${k._1}")
        bw.write(k._1.toString)
        bw.write(" ")
        k._2.mkString(" ")
        bw.write("\n")
    }

    /**
     * Update the knowledge of the environment
     * @param data Data with only datapoint from this environment
     */
    protected def update_env(env: Env_key, data: Map[(Job, String), Map[String, Any]]): Unit = {
        println(s"Updating $env knowledge with ${data.size} samples")
        // Regroup per chunk
        val d_per_chunk: Map[Int, List[Map[String, Any]]] =
            data.values.groupBy(getChunk).mapValues(_.toList)

        // (avg, size) per chunk
        val avg_size_per_chunk: Map[Int, (Double, Int)] =
            d_per_chunk.mapValues(m => (avg(m.map(_("execTime").asInstanceOf[Long])), m.size))
        println(s"$env : (avg, size)")
        avg_size_per_chunk.foreach(println)

        // (ratio, size) between chunks
        val ratio_size_per_chunk: Map[Int, ChunkValue] =
            avg_size_per_chunk.map(t => (t._1, compute_ratio(t._2, avg_size_per_chunk.getOrElse(t._1 + 1, null))))

        // Update + merge
        ratio_size_per_chunk.foreach(t => knowledge(env)(t._1) = mergeRatio(knowledge(env)(t._1), t._2))
    }

    /**
     * Compute the ratio of timing between two chunks
     * i -> i+1
     * @param chunkI Chunk_i
     * @param chunkIp1 Chunk_{i+1}
     * @return (ai+1/ai, min(ni, ni+1))
     */
    protected def compute_ratio(chunkI: (Double, Int), chunkIp1: (Double, Int)): ChunkValue = {
        // FIXME refactor to Option
        if (chunkIp1 != null) {
            return ChunkValue(chunkIp1._1 / chunkI._1, Math.min(chunkI._2, chunkIp1._2))
        }

        ChunkValue(-1.0, 0)
    }

    /**
     * Merge the two tuple of (ratio, weight)
     * @param r1 First tuple
     * @param r2 Second tuple
     * @return New tuple
     */
    protected def mergeRatio(r1: ChunkValue, r2: ChunkValue): ChunkValue = {
        ChunkValue((r1.ratio * r1.weight + r2.ratio * r2.weight) / (r1.weight + r2.weight),
            r1.weight + r2.weight)
    }

    /**
     * Compute the average value of the list
     * @param l The list
     * @return The average
     */
    protected def avg(l: List[Long]): Double = {
        l.sum.toDouble / l.size
    }
}

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

abstract class GlobalStrategy {
    protected var path: String = null
    protected val knowledge = new mutable.HashMap[(String, String), Array[(Double, Int)]]()

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
     * @param p file
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
            case ex: Exception => // No such file
        }

        knowledge.foreach(println)
    }

    /**
     * Create a new list (of knowledge) for the environment
     * @param env The environment to add
     */
    protected def add_environment(env: SimpleBatchEnvironment) {
        val key = envToKey(env)
        add_environment(key._1, key._2)
    }

    protected def add_environment(kind: String, address: String): Unit = {
        knowledge((kind, address)) = Array.fill(83) { (-1.0, 0) }
    }

    /**
     * Load the environment: create array and fill it with given data
     * @param kind The kind of the environment
     * @param address The address of the environment
     * @param data The data of the environment
     */
    protected def load_environment(kind: String, address: String, data: Array[String]) {
        knowledge((kind, address)) = data.map(_.split(",")).map(a => (a(0).toDouble, a(1).toInt))
    }

    /**
     * Generate the keys for an environment
     * @param env The environment
     * @return (Kind, address)
     */
    protected def envToKey(env: SimpleBatchEnvironment): (String, String) = {
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

        (env_kind, address)
    }

    /**
     * Update and save the knowledge.
     * Use the previous set path variable (with function load)
     * @param data The data needed to update the knowledge
     */
    def save(data: Map[(Job, String), Map[String, Any]], lenv: List[SimpleBatchEnvironment]) {
        // Update for each environment
        lenv.foreach(e =>
            update_env(envToKey(e), data.filter(_._2("senv").asInstanceOf[String] == e.toString)))

        /* Save */
        // (Re)create File
        val file = new File(path)
        val bw = new BufferedWriter(new FileWriter(file))

        // Loop to write line by line
        knowledge.foreach(writeKnowledge(bw, _))
    }

    /**
     * Write the knowledge of the given environment
     * @param bw The writer where to write
     * @param k The knowledge to write
     */
    protected def writeKnowledge(bw: BufferedWriter, k: ((String, String), Array[(Double, Int)])) {
        bw.write(k._1._1)
        bw.write(" ")
        bw.write(k._1._2)

        for (t <- k._2) {
            bw.write(" ")
            bw.write(t._1.toString)
            bw.write(",")
            bw.write(t._2.toString)
        }
    }

    /**
     * Update the knowledge of the environment
     * @param data Data with only datapoint from this environment
     */
    protected def update_env(env: (String, String), data: Map[(Job, String), Map[String, Any]]): Unit = {
        // Regroup per chunk
        val d_per_chunk: Map[Int, List[Map[String, Any]]] = data.values.groupBy(getChunk).mapValues(_.toList)

        // (avg, size) per chunk
        val avg_size_per_chunk: Map[Int, (Double, Int)] = d_per_chunk.mapValues(m => (avg(m.map(_("execTime").asInstanceOf[Long])), m.size))

        // (ratio, size) between chunk
        val ratio_size_per_chunk: Map[Int, (Double, Int)] = avg_size_per_chunk.map(t => (t._1, compute_ratio(t._2, avg_size_per_chunk(t._1 + 1))))

        // Update
        for (i <- 0 to 83) {
            knowledge(env)(i) = mergeRatio(knowledge(env)(i), ratio_size_per_chunk(i))
        }
    }

    /**
     * Compute the ratio of timing between two chunks
     * i -> i+1
     * @param chunkI Chunk_i
     * @param chunkIp1 Chunk_{i+1}
     * @return (ai+1/ai, min(ni, ni+1))
     */
    protected def compute_ratio(chunkI: (Double, Int), chunkIp1: (Double, Int)): (Double, Int) = {
        (chunkIp1._1 / chunkI._1, Math.min(chunkI._2, chunkIp1._2))
    }

    /**
     * Merge the two tuple of (ratio, weight)
     * @param r1 First tuple
     * @param r2 Second tuple
     * @return New tuple
     */
    protected def mergeRatio(r1: (Double, Int), r2: (Double, Int)): (Double, Int) = {
        ((r1._1 * r1._2 + r2._1 * r2._2) / (r1._2 + r2._2), r1._2 + r2._2)
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

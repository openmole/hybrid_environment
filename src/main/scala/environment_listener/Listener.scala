package environment_listener

import scala.collection.mutable
import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment

import org.openmole.tool.file._

import scala.concurrent.stm._

object Listener extends Logger {
    private val env_list = mutable.MutableList[Environment]()
    private val data_store = TMap[String, TMap[String, Any]]()
    private var metrics: mutable.MutableList[String] = null
    var csv_path: String = "/tmp/openmole.csv"

    /**
     * Register a new environment that will be listened to
     * Still need to call start_monitoring to actually start to listen
     * @param env The environment to listen
     */
    def registerEnvironment(env: Environment) {
        env_list += env
    }

    /**
     * Register a collection of new environments that will be listened to
     * Still need to call start_monitoring to actually start to listen
     * @param envs The environments to monitor
     */
    def registerEnvironments(envs: Environment*) {
        env_list ++= envs
    }

    /**
     * Launch a new thread of EnvListener for each environment registered
     */
    def startMonitoring() {
        for (env <- env_list) {
            new EnvListener(env).run()
        }
    }

    /**
     * Create the instance of the Tmap for the job id
     * @param job_id The job id
     */
    def createJobMap(job_id: String) = atomic { implicit ctx =>
        if (data_store contains job_id) {
            Log.logger.severe(s"$job_id already created")
        }
        data_store(job_id) = TMap[String, Any]()
    }

    /**
     * Will write a value in the data_store
     *
     * NEEDS to be rewritten, does nothing at the moment
     *
     * @param job_id The job id
     * @param m The metric name
     * @param v The actual value
     */
    def put(job_id: String, m: String, v: Any) = atomic { implicit ctx =>
        data_store(job_id)(ctx)(m) = v
    }

    // TODO Create class for IO
    /**
     * Will print all the data contained in the data_store
     * Should be replaced by a function writing everything in a file
     */
    def printData() = atomic { implicit ctx =>
        // FIXME Find a way in the openmole script to call this function at the end
        Log.logger.info("Printing data...")

        data_store.keys.foreach(printJob)
    }

    /**
     * Print all the informations stored about the job_id.
     * @param job_id The job to display
     */
    def printJob(job_id: String) = atomic { implicit ctx =>
        println(s"Job: $job_id")
        for (metric: String <- data_store(job_id).keys) {
            println(s"\t$metric : ${data_store(job_id)(ctx)(metric)}")
        }
    }

    /**
     * Dump the data store in the given csv file.
     * File will be created if does not exist, otherwise will append to it.
     * @param path The path to the csv file
     * @return
     */
    def dumpToCSV(path: String) = atomic { implicit ctx =>
        Log.logger.info(s"Dumping data store to $csv_path")
        val file: File = new File(csv_path)

        if (!file.exists()) {
            createCSV(file)
        } else if (metrics == null) {
            initMetrics()
        }

        for (job_id: String <- data_store.keySet) {
            writeJobCSV(job_id, file)
        }
    }

    /**
     * Write all the measurements of the job to the given csv file.
     * @param job_id The job to be written.
     */
    def jobCSV(job_id: String) {
        Log.logger.info(s"Writing job $job_id measurements to $csv_path.")
        val file: File = new File(csv_path)

        if (!file.exists()) {
            createCSV(file)
        } else if (metrics == null) {
            initMetrics()
        }

        writeJobCSV(job_id, file)
    }

    /**
     * Actually write the job_id measures in the file.
     * The file _must_ be created before.
     * @param job_id The job to be written.
     * @param file The destination file.
     */
    private def writeJobCSV(job_id: String, file: File) = atomic { implicit ctx =>

        file.withWriter(true) { writer =>
            for (metric: String <- metrics) {
                writer.append(data_store(job_id)(ctx)(metric).toString)
                writer.append(",")
            }
            writer.append("\n")
        }
    }

    /**
     * Create the csv file, and then call the writeHeader function
     * @param file The file to be created
     */
    private def createCSV(file: File) {
        Log.logger.info(s"Creating the file $file.")
        file.getParentFile.mkdirs
        file.createNewFile

        writeHeader(file)
    }

    /**
     * Put the header (name of the metrics) to the given file
     * @param file The file where the header should be put
     */
    private def writeHeader(file: File) {
        println("Writing header")

        initMetrics()
        file.withWriter(true) { writer =>
            for (metric: String <- metrics) {
                println(metric)
                writer.append(metric)
                writer.append(",")
            }
            writer.append("\n")
        }
    }

    /**
     * Initialize the metrics attribute.
     * Will take every metrics contained in the first datapoint, and sort them.
     * If the metrics for waitingTime, execTime and totalTime are not in there, add them.
     */
    private def initMetrics() = atomic { implicit ctx =>
        metrics = mutable.MutableList[String]()
        metrics ++= data_store(data_store.keySet.head).keys.toList

        // Not great but avoid hardcoding the whole list
        metrics ++= List("waitingTime", "execTime", "totalTime", "failed").filterNot(metrics.contains)

        metrics.sorted
    }
}
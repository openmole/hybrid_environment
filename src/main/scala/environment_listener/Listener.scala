package environment_listener

import scala.collection.mutable
import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.batch.environment.{ SimpleBatchEnvironment, BatchEnvironment }
import org.openmole.core.workflow.job.Job

import org.openmole.tool.file._

import scala.concurrent.stm._

import predictron.{ PredictStrategy, AvgStrat }

object Listener extends Logger {
    private val env_list = mutable.MutableList[Environment]()
    private val data_store = TMap[Job, TMap[String, Any]]()
    private var metrics: mutable.MutableList[String] = null
    var csv_path: String = "/tmp/openmole.csv"

    private type t_callback = (List[(SimpleBatchEnvironment, Double)] => Unit)
    private var callback: t_callback = null
    private val completedJob = mutable.MutableList[Job]()
    private val callThreshold = 10
    private var strat: PredictStrategy = null

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
            new BatchListener(env.asInstanceOf[BatchEnvironment]).run()
        }
    }

    /**
     * Create the instance of the Tmap for the job
     * @param job The job
     */
    def createJobMap(job: Job) = atomic { implicit ctx =>
        if (data_store contains job) {
            //            Log.logger.severe(s"$job_id already created")
        } else {
            data_store(job) = TMap[String, Any]()
        }
    }

    /**
     * Will write a value in the data_store
     *
     * @param job The job
     * @param m The metric name
     * @param v The actual value
     */
    def put(job: Job, m: String, v: Any) = atomic { implicit ctx =>
        //        println(s"Put: $job_id $m = $v")
        data_store(job)(ctx)(m) = v
    }

    /**
     * Increment the counter of completed jobs.
     * Should be called only for completed jobs, not failed ones.
     */
    def completeJob(job: Job) = atomic { implicit ctx =>
        completedJob += job

        if (completedJob.size == callThreshold && callback != null) {
            predictAndCall()
        }
    }

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
     * @param job The job to display
     */
    def printJob(job: Job) = atomic { implicit ctx =>
        println(s"Job: $job")
        for (metric: String <- data_store(job).keys) {
            println(s"\t$metric : ${data_store(job)(ctx)(metric)}")
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

        for (job: Job <- data_store.keySet) {
            writeJobCSV(job, file)
        }
    }

    /**
     * Write all the measurements of the job to the given csv file.
     * @param job The job to be written.
     */
    def jobCSV(job: Job) {
        Log.logger.info(s"Writing job $job measurements to $csv_path.")
        val file: File = new File(csv_path)

        if (!file.exists()) {
            createCSV(file)
        } else if (metrics == null) {
            initMetrics()
        }

        writeJobCSV(job, file)
    }

    /**
     * Actually write the job_id measures in the file.
     * The file _must_ be created before.
     * @param job The job to be written.
     * @param file The destination file.
     */
    private def writeJobCSV(job: Job, file: File) = atomic { implicit ctx =>

        file.withWriter(true) { writer =>
            for (metric: String <- metrics) {
                writer.append(data_store(job)(ctx)(metric).toString)
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
        metrics ++= List("waitingTime", "execTime", "totalTime", "failed", "uploadTime").filterNot(metrics.contains)

        metrics = metrics.sorted
    }

    /**
     * The callback function will be called (if defined) when the Listener got enough data to predict execution time
     * @param fct The function to be called.
     */
    def register_callback(fct: t_callback) = {
        callback = fct
    }

    /**
     * Will predict the times, and then call the callback, giving it the predictions as parameters.
     */
    private def predictAndCall() = {
        val data = genDataPredict()

        strat = new AvgStrat

        val el = env_list.toList.map(e => e.asInstanceOf[SimpleBatchEnvironment])
        val predictions = strat.predict(data, el)

        callback(predictions)
    }

    /**
     * Change the data_structure for the prediction module
     * From Tmap to map
     * @return The new data structure
     */
    private def genDataPredict(): Map[Job, Map[String, Any]] = atomic { implicit ctx =>
        //        var res = Map[Job, Map[String, Any]]()

        //        val df = data_store.filter(j => completedJob.contains(j)).mapValues(m => m.toMap).toMap

        data_store.filter(j => completedJob.contains(j)).mapValues(m => m.toMap).toMap
    }
}
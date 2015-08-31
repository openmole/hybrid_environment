package environment_listener

import java.io.File

import environment_listener.Listener._
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.job.Job
import org.openmole.tool.file._
import org.openmole.tool.logger.Logger

import scala.collection.mutable
import scala.concurrent.stm._

trait ListenerWriter {
    protected val data_store = TMap[(Job, Environment), TMap[String, Any]]()
    protected var metrics: mutable.MutableList[String] = null
    var csv_path: String = "/tmp/openmole.csv"

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
     * @param je The job to display
     */
    def printJob(je: (Job, Environment)) = atomic { implicit ctx =>
        println(s"Job: ${je._1}")
        for (metric: String <- data_store(je).keys) {
            println(s"\t$metric : ${data_store(je)(ctx)(metric)}")
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

        for (je: (Job, Environment) <- data_store.keySet) {
            writeJobCSV(je, file)
        }
    }

    /**
     * Write all the measurements of the job to the given csv file.
     * @param job The job to be written.
     */
    def jobCSV(job: Job, env: Environment) {
        //        Log.logger.info(s"Writing job $job measurements to $csv_path.")
        val file: File = new File(csv_path)

        if (!file.exists()) {
            createCSV(file)
        } else if (metrics == null) {
            initMetrics()
        }

        writeJobCSV((job, env), file)
    }

    /**
     * Actually write the job_id measures in the file.
     * The file _must_ be created before.
     * @param je The job to be written.
     * @param file The destination file.
     */
    private def writeJobCSV(je: (Job, Environment), file: File) = atomic { implicit ctx =>

        file.withWriter(true) { writer =>
            for (metric: String <- metrics) {
                writer.append(data_store(je)(ctx)(metric).toString)
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

        metrics = metrics.sorted
    }
}

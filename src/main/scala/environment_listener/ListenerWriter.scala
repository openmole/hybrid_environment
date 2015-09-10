package environment_listener

/** Gathers all I/O operations for the Listenner */
trait ListenerWriter {
    protected val data_store = TMap[(Job, String), TMap[String, Any]]()
    /* By being public, allow user to select which variables he wants in the csv
    * Also allow him to change the order */
    var metrics: List[String] = List("env_kind", "env_name", "day_w",
        "hour", "waitingTime", "execTime",
        "totalTime", "failed", "uploadTime",
        "id", "senv")
    var csv_path: String = "/tmp/openmole.csv"


    /**
     * Print all the informations stored about the job_id.
     * Used for debug purpose
     * @param je The job to display
     */
    def printJob(je: (Job, String)) = atomic { implicit ctx =>
        println(s"Job: ${je._1}")
        for (metric: String <- data_store(je).keys) {
            println(s"\t$metric : ${data_store(je)(ctx)(metric)}")
        }
    }

    /**
     * Dump the data store in the given csv file.
     * File will be created if does not exist, otherwise will append to it.
     * Unused at the moment (csv is written per job).
     * @param path The path to the csv file
     * @return
     * @deprecated
     */
    @deprecated
    def dumpToCSV(path: String) = atomic { implicit ctx =>
        Log.logger.fine(s"Dumping data store to $csv_path")
        val file: File = new File(csv_path)

        this.synchronized {
            if (!file.exists()) {
                createCSV(file)
            }
        }

        for (je: (Job, String) <- data_store.keySet) {
            writeJobCSV(je, file)
        }
    }

    /**
     * Write all the measurements of the job to the given csv file.
     * @param job The job to be written.
     */
    def jobCSV(job: Job, env: Environment) {
        Log.logger.fine(s"Writing job $job measurements to $csv_path.")

        val file: File = new File(csv_path)

        this.synchronized {
            if (!file.exists()) {
                createCSV(file)
            }
        }

        writeJobCSV((job, env.toString()), file)
    }

    /**
     * Actually write the job_id measures in the file.
     * The file _must_ be created before.
     * @param je The job to be written.
     * @param file The destination file.
     */
    private def writeJobCSV(je: (Job, String), file: File) = atomic { implicit ctx =>

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
        Log.logger.fine(s"Creating the file $file.")
        file.getParentFile.mkdirs
        file.createNewFile

        writeHeader(file)
    }

    /**
     * Put the header (name of the metrics) to the given file
     * @param file The file where the header should be put
     */
    private def writeHeader(file: File) {
        Log.logger.fine("Writing CSV header")

        file.withWriter(true) { writer =>
            for (metric: String <- metrics) {
                println(metric)
                writer.append(metric)
                writer.append(",")
            }
            writer.append("\n")
        }
    }
}

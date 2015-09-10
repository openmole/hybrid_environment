package environment_listener

import hybrid.HybridEnvironment
import org.openmole.core.workflow.puzzle.Puzzle

import scala.collection.mutable
import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.batch.environment.{ SimpleBatchEnvironment, BatchEnvironment }
import org.openmole.core.workflow.job.Job

import scala.concurrent.stm._

object Listener extends Logger with ListenerWriter {
    val env_list = mutable.MutableList[Environment]()

    private type t_callback = (Map[(Job, String), Map[String, Any]] => Unit)
    private var completedJob = mutable.MutableList[(Job, Environment)]()
    private val cJobPerEnv = TMap[Environment, Long]()

    /* Are initialized by registerCallback*/
    private var callThreshold: Long = -1
    private var callback: t_callback = null

    var hyb: HybridEnvironment = null

    /**
     * Run and listen the given puzzle
     * @param p The puzzle to listen to.
     */
    def listenPuzzle(p: Puzzle) {
        println("Listening to puzzle")

        val me = p.toExecution
        me.start
        println("Puzzle started")

        new MoleListener(me, hyb).run()

        me.waitUntilEnded

        hyb.globalStrategy.save(Listener.exportCompletedJobs(),
            Listener.env_list.toList.map(_.asInstanceOf[SimpleBatchEnvironment]))
        println("Puzzle ended")
    }

    /**
     * Register a new environment that will be listened to
     * Still need to call start_monitoring to actually start to listen
     * @param env The environment to listen
     */
    def registerEnvironment(env: Environment) {
        env_list += env
    }

    /**
     * Remove every data from the data_store
     *
     */
    def flush_data_store() = atomic { implicit ctx =>
        data_store.keys.foreach(data_store.remove)
        completedJob = mutable.MutableList[(Job, Environment)]()
        println(s"Data store flushed: ${data_store.size}")
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

            atomic { implicit ctx =>
                cJobPerEnv(env) = 0
            }
        }
    }

    /**
     * Create the instance of the Tmap for the job
     * @param job The job
     */
    def createJobMap(job: Job, env: Environment) = atomic { implicit ctx =>
        if (data_store contains (job, env.toString())) {
            //            println(s"$job in $env already created")
        } else {
            data_store((job, env.toString())) = TMap[String, Any]()
        }
    }

    /**
     * Will write a value in the data_store
     *
     * @param job The job
     * @param m The metric name
     * @param v The actual value
     */
    def put(job: Job, env: Environment, m: String, v: Any) = atomic { implicit ctx =>
        //        println(s"Put: $job_id $m = $v")
        //        println(s"Getting $m for ($job, $env)")
        if (!env_list.contains(env)) {
            println(s"Error: $env is not contained is the list")
        }
        data_store((job, env.toString()))(ctx)(m) = v
    }

    /**
     * Add the job to the list of completed job.
     * Update the counter of completed job per environment
     * Once number of completed > threshold (and each environment got at least one completed)
     *  will call the predictAndCall function.
     * @see callThreshold
     * @see predictAndCall
     * @param job The job completed
     * @param env The environment where is happened
     */
    def completeJob(job: Job, env: Environment) = atomic { implicit ctx =>
        if (callback != null) {
            this.synchronized {
                completedJob += ((job, env))
            }

            cJobPerEnv(env) = cJobPerEnv(env) + 1

            if ((completedJob.size % callThreshold) == 0 && cJobPerEnv.forall(_._2 > 0)) {
                println("Will callback")
                atomic { implicit ctx =>
                    cJobPerEnv.foreach(println)
                }

                callback(exportCompletedJobs())
                println("...")
            }
        }
    }

    /**
     * The callback function will be called (if defined) when the Listener got enough data to predict execution time
     * @param fct The function to be called.
     */
    def registerCallback(fct: t_callback, feedback_size: Long) = {
        callback = fct
        callThreshold = feedback_size
    }

    /**
     * Will export the data of the completed jobs (contained in the completedJob list
     * @return The new data structure with the data
     */
    def exportCompletedJobs(): Map[(Job, String), Map[String, Any]] = atomic { implicit ctx =>
        val tmp = data_store.mapValues(m => m.toMap).toMap.filter(j => completedJob.map(_._1).contains(j._1._1))
        tmp
    }
}
package environment_listener

import scala.collection.mutable
import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.batch.environment.{ SimpleBatchEnvironment, BatchEnvironment }
import org.openmole.core.workflow.job.Job

import org.openmole.tool.file._

import scala.concurrent.stm._

import predictron.{ PredictStrategy, ProrataStrat }

object Listener extends Logger with ListenerWriter {
    private val env_list = mutable.MutableList[Environment]()

    private type t_callback = (List[(SimpleBatchEnvironment, Double)] => Unit)
    private var callback: t_callback = null // Init by registerCallback, reset by predictAndCall
    private val completedJob = mutable.MutableList[(Job, Environment)]()
    private val cJobPerEnv = TMap[Environment, Long]()
    private val callThreshold: Long = 30
    private var strat: PredictStrategy = null // Is initialized in predictAndCall

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
        if (data_store contains job) {
            //            Log.logger.severe(s"$job_id already created")
        } else {
            data_store((job, env)) = TMap[String, Any]()
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
        data_store((job, env))(ctx)(m) = v
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
            if (!data_store.keySet.map(_._1).contains(job)) {
                println("Wut: $job not in data store")
            }

            if (!completedJob.map(_._1).contains(job)) {
                completedJob += ((job, env))
                cJobPerEnv(env) = cJobPerEnv(env) + 1

                if ((completedJob.size % callThreshold) == 0 && cJobPerEnv.forall(_._2 > 0)) {
                    println("Will callback")
                    atomic { implicit ctx =>
                        cJobPerEnv.foreach(println)
                    }
                    predictAndCall()
                }
            }
        }
    }

    /**
     * The callback function will be called (if defined) when the Listener got enough data to predict execution time
     * @param fct The function to be called.
     */
    def registerCallback(fct: t_callback) = {
        callback = fct
    }

    /**
     * Will predict the times, and then call the callback, giving it the predictions as parameters.
     * callback will be reset (null) at the end of the function.
     */
    private def predictAndCall() = {
        println(s"Size job completed ${completedJob.size}")
        val data = genDataPredict()
        println(s"Size data predict: ${data.keySet.size}")

        strat = new ProrataStrat

        val el = env_list.toList.map(e => e.asInstanceOf[SimpleBatchEnvironment])
        val predictions = strat.predict(data, el)

        callback(predictions)
    }

    /**
     * Change the data_structure for the prediction module
     * From Tmap to map
     * @return The new data structure
     */
    private def genDataPredict(): Map[(Job, Environment), Map[String, Any]] = atomic { implicit ctx =>
        val tmp = data_store.mapValues(m => m.toMap).toMap.filter(j => completedJob.contains(j._1))
        tmp
    }
}
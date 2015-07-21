package environment_listener

import scala.collection.mutable
import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment

import scala.concurrent.stm._

object Listener extends Logger{
    private val env_list = mutable.MutableList[Environment]()
	private val data_store = TMap[String, TMap[String, Any]]()


    /**
     * Register a new environment that will be listened to
     * Still need to call start_monitoring to actually start to listen
     * @param env The environment to listen
     */
	def register_env(env: Environment) = {
        env_list += env
    }


    /**
     * Launch a new thread of EnvListener for each environment registered
     */
    def start_monitoring() = atomic { implicit ctx =>
        for(env <- env_list){
            new EnvListener(env).run()
        }
    }


    /**
      * Create the instance of the Tmap for the job id
     * @param job_id The job id
     */
    def create_job_map(job_id: String) = atomic { implicit ctx =>
        if(data_store contains job_id){
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


    // FIXME Find a way in the openmole script to call this function at the end
    /**
     * Will print all the data contained in the data_store
     * Should be replaced by a function writing everything in a file
     */
	def print_data() = atomic { implicit ctx =>
        Log.logger.info("Printing data...")

        for(job : String <- data_store.keys) {
            println(s"Job: $job")
            for (metric: String <- data_store(job).keys) {
                println(s"\t$metric : ${data_store(job)(ctx)(metric)}")
            }
        }
	}
}
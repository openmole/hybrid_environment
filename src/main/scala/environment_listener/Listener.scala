package environment_listener

import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment

import scala.concurrent.stm._

object Listener extends Logger{
    // TODO: Change structure of data_store
	private val data_store = TMap[Environment, TMap[String, TMap[String, Any]]]()


    /**
     * Register a new environment that will be listened to
     * Still need to call start_monitoring to actually start to listen
     * @param env The environment to listen
     */
	def register_env(env: Environment) = atomic { implicit ctx =>
		data_store(env) = TMap[String, TMap[String, Any]]()
	}


    /**
     * Will write a value in the data_store
     *
     * NEEDS to be rewritten, does nothing at the moment
     *
     * @param env The environment where the job is running
     * @param job_id The job id
     * @param m The metric name
     * @param v The actual value
     */
    def put(env : Environment, job_id : String, m : String, v : Any) = atomic { implicit ctx =>
        data_store(env)(ctx)(job_id)(ctx)(m) = v
    }

    /**
     * Create the instance of the Tmap for the job id
     * @param env The environment concerned
     * @param job_id The job id
     */
    def create_job_map(env : Environment, job_id : String) = atomic { implicit ctx =>
        data_store(env)(ctx)(job_id) = TMap[String, Any]()
    }


    /**
     * Launch a new thread of EnvListener for each environment registered
     */
	def start_monitoring() = atomic { implicit ctx =>
        for(env <- data_store.keys){
            new EnvListener(env).run()
        }
	}


    /**
     * Will print all the data contained in the data_store
     * Should be replaced by a function writing everything in a file
     */
	def print_datas() = atomic { implicit ctx =>
        Log.logger.info("Printing data...")
		for(env : Environment <- data_store.keys){
			println(s"Env: $env")
            for(job : String <- data_store(env).keys) {
                println(s"\tJob: $job")
                for (metric: String <- data_store(env)(ctx)(job).keys) {
                    println(s"\t\t$metric : ${data_store(env)(ctx)(job)(ctx)(metric)}")
                }
            }
		}
	}
}


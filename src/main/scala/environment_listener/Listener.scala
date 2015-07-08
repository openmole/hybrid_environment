package environment_listener

import org.openmole.core.batch.environment.BatchEnvironment
import org.openmole.core.tools.service.Logger

// import scala.collection.concurrent.Map
import scala.collection.mutable.HashMap

object Listener extends Logger{
    // WHY on earth this is not working with concurrent.Map
    // FIXME concurrent map
	private var data_store = HashMap[BatchEnvironment, HashMap[Long, HashMap[String, Any]]]()


    /**
     * Register a new environment that will be listened to
     * Still need to call start_monitoring to actually start to listen
     * @param env The environment to listen
     */
	def register_env(env: BatchEnvironment) = {
		data_store(env) = HashMap[Long, HashMap[String, Any]]()
	}


    /**
     * Will write a value in the data_store
     * @param env The environment where the job is running
     * @param job_id The job id
     * @param m The metric name
     * @param v The actual value
     */
    def put_value(env : BatchEnvironment, job_id : Long, m : String, v : Any) = {
        if(!data_store(env).keySet.contains(job_id)){
            data_store(env)(job_id) = HashMap[String, Any]()

            // Retrieve in openmole the job caracteristics ?
        }

        data_store(env)(job_id)(m) = v
    }


    /**
     * Launch a new thread of EnvListener for each environment registered
     */
	def start_monitoring() = {
        for(env <- data_store.keys){
            new EnvListener(env).run()
        }
	}


    /**
     * Will print all the data contained in the data_store
     * Should be replaced by a function writing everything in a file
     */
	def print_datas() = {
		for(env : BatchEnvironment <- data_store.keys){
			println(s"Env: $env")
            for(job : Long <- data_store(env).keys) {
                println(s"\tJob: $job")
                for(metric : String <- data_store(env)(job).keys){
                    println(s"\t\t$metric : ${data_store(env)(job)(metric)}")
                }
            }
		}
	}
}


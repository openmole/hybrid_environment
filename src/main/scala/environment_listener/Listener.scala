package environment_listener

import org.openmole.core.batch.environment.BatchEnvironment
import org.openmole.core.tools.service.Logger

// import scala.collection.concurrent.Map
import scala.collection.mutable.HashMap

object Listener extends Logger{
    // WHY on earth this is not working with concurrent.Map
    // FIXME concurrent map
	var data_store = HashMap[BatchEnvironment, String]()

	def add_env(env: BatchEnvironment) = {
		data_store(env) = "toto"
	}

	def start_monitoring() = {
        for(env <- data_store.keys){
            new EnvListener(env).run()
        }
	}


	def print_datas() = {
		for(key <- data_store.keys()){
			println(key)
		}
	}
}


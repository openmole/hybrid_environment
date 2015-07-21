package environment_listener

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable

import org.openmole.core.workflow.execution.{ExecutionJob, Environment}
import org.openmole.core.workflow.execution.ExecutionState.{ExecutionState, SUBMITTED, READY, DONE, KILLED, FAILED, RUNNING}
import org.openmole.core.workflow.execution.Environment.JobStateChanged

import org.openmole.plugin.environment.slurm.SLURMEnvironment
import org.openmole.plugin.environment.ssh.SSHEnvironment
import org.openmole.plugin.environment.condor.CondorEnvironment

import org.openmole.core.event._


object EnvListener {
    /**
     * Will contain the list of all the date measurements done at the submission of the job
     */
    val date_list = mutable.MutableList[(SimpleDateFormat, String)]()

    date_list += ((new SimpleDateFormat("HH"), "hour"))
    date_list += ((new SimpleDateFormat("mm"), "min"))
    date_list += ((new SimpleDateFormat("ss"), "sec"))
    date_list += ((new SimpleDateFormat("dd"), "day_m"))
    date_list += ((new SimpleDateFormat("F"), "day_w"))
    date_list += ((new SimpleDateFormat("Z"), "timezone"))
    date_list += ((new SimpleDateFormat("M"), "month"))
}

class EnvListener(env : Environment) extends Runnable {
    // FIXME ExpectedState
    val jobExpectedState = mutable.HashMap[String, ExecutionState]()
    val jobTimings = mutable.HashMap[String, mutable.HashMap[String, Long]]()

    val shortId = mutable.HashMap[ExecutionJob, String]()
    val L = Listener.Log.logger

    /**
     * The name of the environment.
     * SSH => user@host
     * Condor => user@host
     * SLURM => user@host
     */
    val env_name : String = env match {
        case e:SSHEnvironment =>  e.user ++ "@" ++ e.host
        case e:CondorEnvironment => e.user ++ "@" ++ e.host
        case e:SLURMEnvironment => e.user ++ "@" ++ e.host
        case _ =>
            L.severe(s"Unsupported environment (envName): $env.")
            ""
    }

    /**
     * The kind of the environment.
     * SSH
     * Condor
     * SLURM
     */
    val env_kind : String = env match {
        case _:SSHEnvironment => "SSHEnvironment"
        case _:CondorEnvironment => "CondorEnvironment"
        case _:SLURMEnvironment => "SLURMEnvironment"
        case _ =>
            L.severe(s"Unsupported environment (envKind): $env.")
            ""
    }

    /**
     * The number of core of the environment.
     * SSH
     * Condor
     * SLURM
     */
    val core : Int = env match {
        case e:SSHEnvironment => e.nbSlots
        case e:CondorEnvironment => e.threads.getOrElse(1)
        case e:SLURMEnvironment => e.threads.getOrElse(1)
        case _ =>
            L.severe(s"Unsupported environment (core): $env.")
            -1
    }

    /**
     * Start to monitor the environment
     */
    def run(): Unit = {
        L.info(s"Now monitoring $env.")

        // FIXME: Too spaghetti
        env listen {
            case (_, JobStateChanged(job, SUBMITTED, READY)) =>
                create(job)
                fillInputs(job)
                processNewState(job, SUBMITTED, READY)
            case (_, JobStateChanged(_, RUNNING, RUNNING)) => // ugly way to mask those transition
            case (_, JobStateChanged(job, KILLED, oldState)) =>
                putTimings(job)
                Listener.put(shortId(job), "failed", false)
                Listener.print_data()
            case (_, JobStateChanged(job, FAILED, _)) =>
                putTimings(job)
                Listener.put(shortId(job), "failed", true)
                Listener.print_data()
            case (_, JobStateChanged(job, newState, oldState)) => processNewState(job, newState, oldState)
        }
    }

    /**
     * Create the data structures needed to monitor a job
     * @param job The job
     */
    def create(job: ExecutionJob) = {
        L.info(s"Catched $job.")

        shortId(job) = genShortId(job)
        val id = shortId(job)

        Listener.create_job_map(id)
        jobTimings(id) = new mutable.HashMap[String, Long]
        jobExpectedState(id) = DONE
    }

    /**
     * Fill the first values in the Listener data store.
     * The values are the name of the environment, the kind, number of core
     * And all the values defined in EnvListener.date_list
     * @param job The job
     */
    def fillInputs(job : ExecutionJob) = {
        val id = shortId(job)

        Listener.put(id, "env_name", env_name)
        Listener.put(id, "env_kind", env_kind)
        Listener.put(id, "core", core)

        val t = Calendar.getInstance.getTime
        for((dateFormat, name) <- EnvListener.date_list){
            Listener.put(id, name, dateFormat.format(t))
        }
    }

    /**
     * Generate a shorter id for the job
     * @param job The job
     * @return Everything at the right of the '@'
     */
    def genShortId(job: ExecutionJob) : String = {
        val tmp = job.toString

        tmp.drop(tmp.indexOf('@') + 1)
    }

    /**
     * Will create an entry in the jobTimings table for the new state.
     * @param job The job
     * @param newState The new state
     * @param oldState Mostly irrelevant, only used for the sanityState check.
     */
    def processNewState(job : ExecutionJob, newState : ExecutionState, oldState : ExecutionState) = {
        println(s"$job\n\t$oldState -> $newState")

        sanityState(job, newState, oldState)

        jobTimings(shortId(job))(newState.toString()) = Calendar.getInstance.getTimeInMillis
    }

    /**
     * Should be called once the current state is KILLED or FAILED.
     * It will compute all the defined timings using the saved times in jobTimings
     * At the moment, compute :
     *  totalTime = Done - submitted
     * @param job The job
     */
    def putTimings(job : ExecutionJob) = {
        val id : String = shortId(job)
        val totalTime : Long = jobTimings(id)("Done") - jobTimings(id)("Submitted")

        Listener.put(id, "totalTime", totalTime)
    }

    /**
     * Check if the new state makes sense according to the current knowledge.
     * @param job The job concerned
     * @param newState The new state
     * @param oldState The old state (currently irrelevant in the processing)
     */
    def sanityState(job : ExecutionJob, newState : ExecutionState, oldState : ExecutionState) = {
        val id : String = shortId(job)

        if(id == null){
            L.severe(s"Job unknown: $job\n\tProbably not heard when readied.")
        }
        val es = jobExpectedState(id)
        if(es != newState){
            L.severe(s"New state doesn't match expected state\n\tExpecting $es, found $newState.")
        }
    }
}
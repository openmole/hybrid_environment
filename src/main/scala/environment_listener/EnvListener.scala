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

    // TODO memory

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
            case (_, JobStateChanged(_, SUBMITTED, SUBMITTED)) => // ugly way to mask those transition
            case (_, JobStateChanged(job, KILLED, oldState)) =>
                putTimings(job)
                Listener.put(shortId(job), "failed", false)
                Listener.job_csv(shortId(job))
                Listener.print_job(shortId(job))
                delete(job)
            case (_, JobStateChanged(job, FAILED, _)) =>
                failedTimings(job)
                Listener.put(shortId(job), "failed", true)
                Listener.job_csv(shortId(job))
                delete(job)
            case (_, JobStateChanged(job, newState, oldState)) => processNewState(job, newState, oldState)
        }
    }

    /**
     * Create the data structures needed to monitor a job
     * @param job The job
     */
    def create(job : ExecutionJob) = {
        L.info(s"Catched $job.")

        shortId(job) = genShortId(job)
        val id = shortId(job)

        Listener.create_job_map(id)
        jobTimings(id) = new mutable.HashMap[String, Long]
    }

    /**
     * Delete the (now unnecessary) data structures about the given job
     * Remove entry in jobTimings, and in shortId
     * @param job The job to be deleted
     */
    def delete(job : ExecutionJob) = {
        jobTimings.remove(shortId(job))

        shortId.remove(job)
    }

    /**
     * Fill the first values in the Listener data store.
     * The values are the name of the environment, the kind, number of core
     * And all the values defined in EnvListener.date_list
     * @param job The job
     */
    def fillInputs(job : ExecutionJob) = {
        // TODO Find more inputs about the job itself
        // But like what ? Kind of task ? Content to upload ?
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
     */
    def processNewState(job: ExecutionJob, newState: ExecutionState, oldState : ExecutionState) = {
        L.info(s"$job\n\t$oldState -> $newState")

        jobTimings(shortId(job))(newState.toString()) = Calendar.getInstance.getTimeInMillis / 1000
    }

    /**
     * Differenciate the timings between the two states, and put them in the data store.
     * t(laterState) - t(earlyState)
     * If at least one state is not found, will put 0 as value.
     * @param job The job concerned by the timing.
     * @param name The name of the timing.
     * @param laterState The most recent state.
     * @param earlyState The oldest state.
     */
    def putDiff(job : ExecutionJob, name : String,  laterState : String, earlyState : String) = {
        val id : String = shortId(job)

        var v : Long = 0
        if(jobTimings(id).contains(laterState) && jobTimings(id).contains(earlyState)){
            v = jobTimings(id)(laterState) - jobTimings(id)(earlyState)
        }

        Listener.put(id, name , v)
    }

    /**
     * Should be called once the current state is KILLED.
     * It will compute all the defined timings using the saved times in jobTimings
     * At the moment, compute :
     *  totalTime = Done - submitted
     *  execTime = Done - running
     *  waitingTime = running - submitted
     * @param job The job
     */
    def putTimings(job : ExecutionJob) = {
        putDiff(job, "totalTime", DONE.toString(), SUBMITTED.toString())
        putDiff(job, "execTime", DONE.toString(), RUNNING.toString())
        putDiff(job, "waitingTime", RUNNING.toString(), SUBMITTED.toString())
    }

    /**
     * Does the same at putTimings, but in the case of a failed job
     * Does:
     *  totalTime = failed - submitted
     *  execTime = failed - running
     *  waitingTime = running - submitted
     * @param job The job that failed
     */
    def failedTimings(job : ExecutionJob) = {
        putDiff(job, "totalTime", FAILED.toString(), SUBMITTED.toString())
        putDiff(job, "execTime", FAILED.toString(), RUNNING.toString())
        putDiff(job, "waitingTime", RUNNING.toString(), SUBMITTED.toString())
    }
}
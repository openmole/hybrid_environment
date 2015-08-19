package environment_listener

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable

import org.openmole.core.event._

import org.openmole.core.workflow.execution.{ ExecutionJob, Environment }
import org.openmole.core.workflow.execution.ExecutionState.{ ExecutionState, SUBMITTED, READY, DONE, KILLED, FAILED, RUNNING }
import org.openmole.core.workflow.execution.Environment.JobStateChanged
import org.openmole.core.workflow.job.Job
import org.openmole.core.batch.environment.BatchExecutionJob

import org.openmole.plugin.environment.slurm.SLURMEnvironment
import org.openmole.plugin.environment.ssh.SSHEnvironment
import org.openmole.plugin.environment.condor.CondorEnvironment
import org.openmole.plugin.environment.egi.EGIEnvironment

object EnvListener {
    /**
     * Will contain the list of all the date measurements done at the submission of the job
     */
    private val date_list = mutable.MutableList[(SimpleDateFormat, String)]()

    date_list += ((new SimpleDateFormat("HH"), "hour"))
    date_list += ((new SimpleDateFormat("mm"), "min"))
    date_list += ((new SimpleDateFormat("ss"), "sec"))
    date_list += ((new SimpleDateFormat("dd"), "day_m"))
    date_list += ((new SimpleDateFormat("F"), "day_w"))
    date_list += ((new SimpleDateFormat("Z"), "timezone"))
    date_list += ((new SimpleDateFormat("M"), "month"))
}

class EnvListener(env: Environment) extends Runnable {
    private val jobTimings = mutable.HashMap[Job, mutable.HashMap[String, Long]]()

    private val jobJob = mutable.HashMap[ExecutionJob, Job]()
    private val L = Listener.Log.logger
    private var n: Long = 0

    /**
     * The name of the environment.
     * SSH => user@host
     * Condor => user@host
     * SLURM => user@host
     * EGI => virtual organization name
     */
    private val env_name: String = env match {
        case e: SSHEnvironment => e.user ++ "@" ++ e.host
        case e: CondorEnvironment => e.user ++ "@" ++ e.host
        case e: SLURMEnvironment => e.user ++ "@" ++ e.host
        case e: EGIEnvironment => e.voName
        case _ =>
            L.severe(s"Unsupported environment (envName): $env.")
            ""
    }

    /**
     * The kind of the environment.
     * SSH
     * Condor
     * SLURM
     * EGI
     */
    private val env_kind: String = env match {
        case _: SSHEnvironment => "SSHEnvironment"
        case _: CondorEnvironment => "CondorEnvironment"
        case _: SLURMEnvironment => "SLURMEnvironment"
        case _: EGIEnvironment => "EGIEnvironment"
        case _ =>
            L.severe(s"Unsupported environment (envKind): $env.")
            ""
    }

    /**
     * The number of core of the environment.
     * SSH
     * Condor
     * SLURM
     * EGI
     */
    private val core: Int = env match {
        case e: SSHEnvironment => e.nbSlots
        case e: CondorEnvironment => e.threads.getOrElse(1)
        case e: SLURMEnvironment => e.threads.getOrElse(1)
        case e: EGIEnvironment => e.threads.getOrElse(1)
        case _ =>
            L.severe(s"Unsupported environment (core): $env.")
            -1
    }

    // TODO Improve default case
    private val memory: Int = env match {
        case e: SSHEnvironment => 0
        case e: CondorEnvironment => e.memory.getOrElse(0)
        case e: SLURMEnvironment => e.memory.getOrElse(0)
        case e: EGIEnvironment => e.memory.getOrElse(0)
        case _ =>
            L.severe(s"Unsupported environment (memory): $env.")
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
            case (_, JobStateChanged(job, KILLED, oldState)) =>
                putTimings(job)
                Listener.put(jobJob(job), "failed", false)
                //                Listener.printJob(shortId(job))
                Listener.jobCSV(jobJob(job))
                delete(job)
            case (_, JobStateChanged(job, FAILED, os)) =>
                processNewState(job, FAILED, os)
                failedTimings(job)
                Listener.put(jobJob(job), "failed", true)
                Listener.jobCSV(jobJob(job))
                delete(job)
            case (_, JobStateChanged(job, newState, oldState)) => processNewState(job, newState, oldState)
        }
    }

    /**
     * Create the data structures needed to monitor a job
     * @param job The job
     */
    private def create(job: ExecutionJob) = {
        L.info(s"Catched ${job.asInstanceOf[BatchExecutionJob].job}.")

        jobJob(job) = job.asInstanceOf[BatchExecutionJob].job
        val id = jobJob(job)
        println(s"$job\n\t${jobJob(job)}")

        Listener.createJobMap(id)
        jobTimings(id) = new mutable.HashMap[String, Long]
    }

    /**
     * Delete the (now unnecessary) data structures about the given job
     * Remove entry in jobTimings, and in shortId
     * @param job The job to be deleted
     */
    private def delete(job: ExecutionJob) = {
        jobTimings.remove(jobJob(job))

        jobJob.remove(job)
    }

    /**
     * Fill the first values in the Listener data store.
     * The values are the name of the environment, the kind, number of core
     * And all the values defined in EnvListener.date_list
     * @param job The job
     */
    private def fillInputs(job: ExecutionJob) = {
        val id = jobJob(job)

        Listener.put(id, "env_name", env_name)
        Listener.put(id, "env_kind", env_kind)
        Listener.put(id, "core", core)
        Listener.put(id, "memory", memory)

        Listener.put(id, "id", genShortId(job))
        Listener.put(id, "group", job.moleJobs.size)

        Listener.put(id, "id", genShortId(job))

        val t = Calendar.getInstance.getTime
        for ((dateFormat, name) <- EnvListener.date_list) {
            Listener.put(id, name, dateFormat.format(t))
        }
    }

    /**
     * Generate a shorter id for the job
     * Cause problem with EGI, since it'll replicate the same job, which will eventually to conflict.
     * Need to find a way to manage it efficiently
     * @param job The job
     * @return Everything at the right of the '@'
     */
    private def genShortId(job: ExecutionJob): String = {
        val tmp = job.asInstanceOf[BatchExecutionJob].job.toString

        tmp.drop(tmp.indexOf('@') + 1)
    }

    /**
     * Will create an entry in the jobTimings table for the new state.
     * @param job The job
     * @param newState The new state
     */
    private def processNewState(job: ExecutionJob, newState: ExecutionState, oldState: ExecutionState) = {
        //        L.info(s"$job\n\t$oldState -> $newState")

        jobTimings(jobJob(job))(newState.toString()) = Calendar.getInstance.getTimeInMillis / 1000
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
    private def putDiff(job: ExecutionJob, name: String, laterState: String, earlyState: String) = {
        val id: Job = jobJob(job)

        var v: Long = 0
        if (jobTimings(id).contains(laterState) && jobTimings(id).contains(earlyState)) {
            v = jobTimings(id)(laterState) - jobTimings(id)(earlyState)
        }

        Listener.put(id, name, v)
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
    private def putTimings(job: ExecutionJob) = {
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
    private def failedTimings(job: ExecutionJob) = {
        putDiff(job, "totalTime", FAILED.toString(), SUBMITTED.toString())
        putDiff(job, "execTime", FAILED.toString(), RUNNING.toString())
        putDiff(job, "waitingTime", RUNNING.toString(), SUBMITTED.toString())
    }
}
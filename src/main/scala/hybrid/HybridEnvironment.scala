/*
 * Copyright (C) 2015 Jonathan Passerat-Palmbach
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package hybrid

import org.openmole.core.workflow.puzzle.Puzzle

import scala.util.Random

import org.openmole.core.batch.environment.{ BatchExecutionJob, SimpleBatchEnvironment }
import org.openmole.core.batch.environment.BatchEnvironment.jobManager
import org.openmole.core.batch.refresh.Manage
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.execution.ExecutionState.{ RUNNING, SUBMITTED }
import org.openmole.core.workflow.job.Job
import org.openmole.core.workspace.AuthenticationProvider
import org.openmole.core.event.EventDispatcher

import environment_listener.{ MoleListener, Listener }
import local_predictron.LocalStrategy
import global_predictron.GlobalStrategy

import HybridEnvironment._

object HybridEnvironment {
    type t_pred = List[(SimpleBatchEnvironment, Double)]

    def apply(
        name: Option[String],
        size_feedback: Long,
        localStrategy: LocalStrategy,
        globalStrategy: GlobalStrategy,
        environmentsList: SimpleBatchEnvironment*)(implicit authentications: AuthenticationProvider) =
        new HybridEnvironment(environmentsList, name, size_feedback, localStrategy, globalStrategy)
}

class HybridEnvironment(
        val environmentsList: Seq[SimpleBatchEnvironment],
        override val name: Option[String] = None,
        val sizeFeedback: Long,
        val localStrategy: LocalStrategy,
        val globalStrategy: GlobalStrategy)(implicit authentications: AuthenticationProvider) extends SimpleBatchEnvironment { env ⇒

    private val rng: (Int => Int) = new Random().nextInt
    private val es = environmentsList.size
    private val reg = batchJobWatcher.registry

    globalStrategy.load("/tmp/global.openmole")
    environmentsList.foreach(globalStrategy.add_environment)
    println("Print init knowledge:")
    globalStrategy.knowledge.foreach(println)

    /**
     * Register each environment to the Listener, and start the monitoring
     * Also register the callback function. Comment to deactivate
     */
    environmentsList.foreach(Listener.registerEnvironment)
    Listener.registerCallback(callback, sizeFeedback)
    Listener.hyb = this
    Listener.startMonitoring()

    /**
     * Run and listen the given puzzle
     * @param p The puzzle to listen to.
     */
    def runAndListen(p: Puzzle) {
        val herp = this
        class runner extends Runnable {
            def run() = {
                println("Listening to puzzle")

                val me = p.toExecution
                me.start
                println("Puzzle started")

                new MoleListener(me, herp).run()

                me.waitUntilEnded
                println("Puzzle ended")
            }
        }

        new runner().run()
    }

    /**
     * Submit the job to a random environment, with a uniform distribution
     * @param job The job to submit
     * @see submit(Job, SimpleBatchEnvironment)
     */
    override def submit(job: Job) = {
        submit(job, environmentsList(rng(es)))
    }

    /**
     * Submit the job to the specified environment
     * Will not use the submit function of the environment
     * The job will be registered to the local jobManager.
     * @param job The job to submit
     * @param env The environment where to submit
     */
    def submit(job: Job, env: SimpleBatchEnvironment) = {
        val bej = new BEJ(job, env)
        EventDispatcher.trigger(env, new Environment.JobSubmitted(bej))
        batchJobWatcher.register(bej)
        jobManager ! Manage(bej)
    }

    /**
     * Needed by the SimpleBatchEnvironment but should never be called:
     * methods from the actual environments will be called instead.
     */
    override def storage: SS = {
        println("Shouldn't be reached: Hybrid storage")
        environmentsList.head.storage.asInstanceOf[SS]
    }

    /**
     * Needed by the SimpleBatchEnvironment but should never be called:
     * methods from the actual environments will be called instead.
     */
    override def jobService: JS = {
        println("Shouldn't be reached: Hybrid job service")
        environmentsList.head.jobService.asInstanceOf[JS]
    }

    /**
     * Function called by the Listener singleton when it got enough data to generate accurate predictions
     * Contains only data about completed jobs
     * Actually implement the whole feedback loop.
     * @param data Data of the completed jobs
     */
    def callback(data: Map[(Job, String), Map[String, Any]]) = {
        println(s"Called back with ${data.size} datapoints")

        println("Will split")
        val splitted: List[List[Map[String, Any]]] = Splitter.split(data)
        println("Splitted")

        val (current_pred, cw, previous_pred, pw): (t_pred, Int, t_pred, Int) =
            localStrategy.predict(splitted, environmentsList.toList)
        println("Current pred:")
        current_pred.foreach(println)

        val u_previous_pred: t_pred =
            globalStrategy.predict(environmentsList.toList, previous_pred)
        println("Previous pred:")
        u_previous_pred.foreach(println)

        val pred: t_pred = Merger.merge(u_previous_pred, pw, current_pred, cw)
        println("Final pred:")
        pred.foreach(println)

        val env_r = compute_repartition(pred)
        println("Repartition:")
        env_r.foreach(println)
        enforce_repartition(env_r)
    }

    /**
     * Calculate, with the predictions, the optimal number of jobs for each environment
     * @param env_pred The time prediction
     * @return Tuples of Environments and number of jobs they should have
     */
    private def compute_repartition(env_pred: List[(SimpleBatchEnvironment, Double)]): List[(SimpleBatchEnvironment, Int)] = {
        val s: Double = env_pred.map(_._2).sum
        val n = reg.allJobs.count(!_.finished)

        // how many jobs should each environment have considering the prediction and amount of jobs still running
        env_pred.map(x => (x._1, (n * (1 - x._2 / s)).toInt))
    }

    /**
     * Will kill and submit job for each environment to fit the given numbers
     * It won't kill running jobs, so the results may slightly differ from the rules
     * @param env_n The rules to follow. Sorted in decreased order
     */
    private def enforce_repartition(env_n: List[(SimpleBatchEnvironment, Int)]) = {
        // TODO : Refactor, this thing is definitely too huge.

        /** Gather exceeding jobs before giving them back to environments */
        var jobPool = List[BatchExecutionJob]()

          // FIXME sort by descending order of exceeding jobs
          // TODO map difference over collecion to sort correctly
//          .sortBy(_._2)

        for ((env, n) <- env_n) {
            val current_n = reg.allExecutionJobs.filter(!_.job.finished).count(_.environment == env)
            val d: Int = current_n - n

            println(s"$env has currently $current_n")
            println(s"$env diff is $d")

            if (d > 0) { // Too much jobs
                jobPool ++= giveJobs(env, d)
            } else if (d < 0) { // Not enough
                jobPool = takeJobs(env, -d, jobPool)
            }
        }
    }

    /**
     * Take jobs from environment.
     * Will try to minimize the number of running jobs.
     * @param env The environment to plunder
     * @param d The number of jobs to take
     * @return List of selected jobs
     */
    private def giveJobs(env: SimpleBatchEnvironment, d: Int): List[BatchExecutionJob] = {
        // First take only submitted
        var toGive = reg.allExecutionJobs.filter(_.environment == env)
            .filter(_.state == SUBMITTED)
            .take(d)

        // If not enough, will take from running
        if (toGive.size < d) {
            toGive ++= reg.allExecutionJobs.filter(_.environment == env)
                .filter(_.state == RUNNING)
                .take(d)
        }

        println(s"$env giving ${toGive.size}")
        toGive.toList
    }

    /**
     * Will take d jobs from the pool, kill them and resubmit them on the environment
     * Will send back the new jobPool
     * @param env The environment to submit
     * @param d The number of jobs to take
     * @param jobPool The pool of jobs
     */
    private def takeJobs(env: SimpleBatchEnvironment, d: Int, jobPool: List[BatchExecutionJob]): List[BatchExecutionJob] = {
        val taken = jobPool.take(d)

        println(s"$env taking ${taken.size}")
        taken.map(_.job).foreach(submit(_, env))
        taken.foreach(jobManager.killAndClean)

        jobPool.drop(d)
    }
}

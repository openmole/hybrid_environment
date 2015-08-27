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

import scala.util.Random
import scala.collection.mutable

import org.openmole.core.batch.environment.{ BatchExecutionJob, SimpleBatchExecutionJob, BatchEnvironment, SimpleBatchEnvironment }
import org.openmole.core.batch.environment.BatchEnvironment.jobManager
import org.openmole.core.batch.refresh.{ JobManager, Manage }
import org.openmole.core.workflow.execution.Environment
import org.openmole.core.workflow.execution.ExecutionState.{ RUNNING, SUBMITTED }
import org.openmole.core.workflow.job.Job
import org.openmole.core.workspace.AuthenticationProvider
import org.openmole.core.event.EventDispatcher

import environment_listener.Listener

object HybridEnvironment {

    def apply(
        // change order to allow variable list as last argument
        // FIXME find a way to have name = None while keeping the variable argument list
        name: Option[String],
        environmentsList: SimpleBatchEnvironment*)(implicit authentications: AuthenticationProvider) =
        new HybridEnvironment(environmentsList, name)
}

class HybridEnvironment(
        val environmentsList: Seq[SimpleBatchEnvironment],
        override val name: Option[String] = None)(implicit authentications: AuthenticationProvider) extends SimpleBatchEnvironment { env ⇒

    val rng: (Int => Int) = new Random().nextInt
    val es = environmentsList.size
    val reg = batchJobWatcher.registry

    /**
     * Register each environment to the Listener, and start the monitoring
     * Also register the callback function. Comment to deactivate
     */
    environmentsList.foreach(Listener.registerEnvironment)
    Listener.registerCallback(callback)
    Listener.startMonitoring()

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

    override def storage: SS = {
        println(s"Shouldn't be there: Hybrid storage")
        environmentsList.head.storage.asInstanceOf[SS]
    }

    override def jobService: JS = {
        println(s"Shouldn't be there: Hybrid job service")
        environmentsList.head.jobService.asInstanceOf[JS]
    }

    /**
     * Function called by the Listener singleton when it got enough data to generate accurate predictions
     * Will decide which environment keep which BatchExecutionJob
     * @param env_pred The list of the predictions
     */
    def callback(env_pred: List[(SimpleBatchEnvironment, Double)]) = {
        println("Called back")
        env_pred.foreach(println)

        println(reg.allJobs.size)
        println(reg.allJobs.count(_.finished))

        // Will now find the best "ratio" to schedule the jobs
        // ex: 0.5 on env1, 0.2 on env2, 0.3 on env3
        val s: Double = env_pred.map(_._2).sum
        val n = reg.allJobs.count(!_.finished)
        println(s)
        println(s"Unfinished: $n")
        val env_n = env_pred.map(x => (x._1, (n * (1 - x._2 / s)).toInt)).sortBy(_._2)

        env_n.foreach(println)

        adjustNumber(env_n)
    }

    /**
     * Will kill and submit job for each environment to fit the given numbers
     * It won't kill running jobs, so the results may slightly differ from the rules
     * @param env_n The rules to follow. Sorted in decreased order
     */
    private def adjustNumber(env_n: List[(SimpleBatchEnvironment, Int)]) = {
        var jobPool = List[BatchExecutionJob]()
        for ((env, n) <- env_n) {
            val current_n = reg.allExecutionJobs.filter(!_.job.finished).count(_.environment == env)
            println(s"$env has currently $current_n")
            var d: Int = current_n - n
            println(s"$env diff is $d")

            if (d > 0) { // Too much jobs
                // Won't take running jobs
                // So may take less than needed
                var toGive = reg.allExecutionJobs.filter(_.environment == env)
                    .filter(_.state == SUBMITTED)
                    .take(d)

                d -= toGive.size
                if (d > 0) {
                    toGive ++= reg.allExecutionJobs.filter(_.environment == env)
                        .filter(_.state == RUNNING)
                        .take(d)
                }

                println(s"$env giving ${toGive.size}")
                jobPool ++= toGive
            } else if (d < 0) { // Not enough
                d = -d

                val taken = jobPool.take(d)
                jobPool = jobPool.drop(d)

                println(s"$env taking ${taken.size}")
                taken.map(_.job).foreach(submit(_, env))
                taken.foreach(jobManager.killAndClean)
            }
        }
    }
}

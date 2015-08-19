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

import org.openmole.core.batch.environment.{BatchEnvironment, SimpleBatchEnvironment }
import org.openmole.core.workflow.job.Job
import org.openmole.core.workspace.AuthenticationProvider

import environment_listener.Listener

object HybridEnvironment {

    def apply(
        // change order to allow variable list as last argument
        // FIXME find a way to have name = None while keeping the variable argument list
        name: Option[String],
        environmentsList: (SimpleBatchEnvironment, Option[Double])*)(implicit authentications: AuthenticationProvider) =
        new HybridEnvironment(environmentsList, name)
}

class HybridEnvironment(
        val environmentsList: Seq[(SimpleBatchEnvironment, Option[Double])],
        override val name: Option[String] = None)(implicit authentications: AuthenticationProvider) extends SimpleBatchEnvironment { env ⇒

    override def submit(job: Job) = {
        environmentsList.foreach(e => e._1.submit(job))

        Listener.register_callback(callback)
    }

    override def storage: SS = {
        println(s"Shouldn't be there: Hybrid storage")
        environmentsList.head._1.storage.asInstanceOf[SS]
    }

    override def jobService: JS = {
        println(s"Shouldn't be there: Hybrid job service")
        environmentsList.head._1.jobService.asInstanceOf[JS]
    }

    /**
     * Function called by the Listener singleton when it got enough data to generate accurate predictions
     * Will decide which environment keep which BatchExecutionJob
     * @param env_pred The list of the predictions
     */
    def callback(env_pred: List[(SimpleBatchEnvironment, Double)]) = {
        val unfinishedJobs = batchJobWatcher.registry.allJobs.filter(!_.finished)

        // Will now find the best "ratio" to schedule the jobs
        // ex: 0.5 on env1, 0.2 on env2, 0.3 on env3
        val s = env_pred.map(_._2).sum
        val r = unfinishedJobs.size
        val env_r = env_pred.map(x => (x._1, (r*(1 - x._2/s)).toInt))

        // Kill the duplicates
        var i = 0
        var t = 0
        for(j <- unfinishedJobs){
            if(t > env_r(i)._2){
                i += 1
            }

            killExcept(j, env_r(i)._1)

            t += 1
        }
    }

    /**
     * Kill every BatchExecutionJob of the job except the one running on the said environment
     * @param job The job concerned
     * @param env The environment to keep
     */
    private def killExcept(job : Job, env : SimpleBatchEnvironment) = {
        batchJobWatcher.registry
                .executionJobs(job)
                .filter(bej => bej.environment.asInstanceOf[SimpleBatchEnvironment] != env)
                .foreach(BatchEnvironment.jobManager.killAndClean)
    }
}

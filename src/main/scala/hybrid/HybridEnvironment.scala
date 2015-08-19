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

import org.openmole.core.batch.environment.{ SimpleBatchEnvironment }
import org.openmole.core.workflow.job.Job
import org.openmole.core.workspace.AuthenticationProvider

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

    // sort by weight to simplify decision
    private val sortedEnvironments = environmentsList.collect {
        case (envName, Some(envWeight)) ⇒ (envName, envWeight)
    }.sortBy(-_._2)

    private val cumulatedWeights = sortedEnvironments.map(_._2).scanLeft(0.0)(_ + _).tail
    private val envtsAndWeights = sortedEnvironments.map(_._1).zip(cumulatedWeights)

    override def submit(job: Job) = {
        environmentsList.foreach(e => e._1.submit(job))

        this.batchJobWatcher.registry.allJobs.filter(!_.finished)
    }

    override def storage: SS = {
        println(s"Shouldn't be there: Hybrid storage")
        environmentsList.head._1.storage.asInstanceOf[SS]
    }

    override def jobService: JS = {
        println(s"Shouldn't be there: Hybrid job service")
        environmentsList.head._1.jobService.asInstanceOf[JS]
    }
}

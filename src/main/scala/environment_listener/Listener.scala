/*
 * Copyright (C) 2015 Hoel Kervadec
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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
package environment_listener

import java.io.{ BufferedWriter, FileWriter, File }

import scala.collection.mutable
import org.openmole.tool.logger.Logger
import org.openmole.core.workflow.execution.Environment

import org.openmole.tool.file._

import scala.concurrent.stm._

object Listener extends Logger {
  private val env_list = mutable.MutableList[Environment]()
  private val data_store = TMap[String, TMap[String, Any]]()
  private val csv_path: String = "/tmp/openmole.csv"

  /**
   * Register a new environment that will be listened to
   * Still need to call start_monitoring to actually start to listen
   * @param env The environment to listen
   */
  def register_env(env: Environment) = {
    env_list += env
  }

  /**
   * Register a collection of new environments that will be listened to
   * Still need to call start_monitoring to actually start to listen
   * @param envs The environments to monitor
   */
  def registerEnvironments(envs: Environment*) = {
    env_list ++= envs
  }

  /**
   * Launch a new thread of EnvListener for each environment registered
   */
  def start_monitoring() = atomic { implicit ctx =>

    for (env <- env_list) {
      new EnvListener(env).run()
    }
  }

  /**
   * Create the instance of the Tmap for the job id
   * @param job_id The job id
   */
  def create_job_map(job_id: String) = atomic { implicit ctx =>
    if (data_store contains job_id) {
      Log.logger.severe(s"$job_id already created")
    }
    data_store(job_id) = TMap[String, Any]()
  }

  /**
   * Will write a value in the data_store
   *
   * NEEDS to be rewritten, does nothing at the moment
   *
   * @param job_id The job id
   * @param m The metric name
   * @param v The actual value
   */
  def put(job_id: String, m: String, v: Any) = atomic { implicit ctx =>
    data_store(job_id)(ctx)(m) = v
  }

  // TODO Create class for IO
  /**
   * Will print all the data contained in the data_store
   * Should be replaced by a function writing everything in a file
   */
  def print_data() = atomic { implicit ctx =>
    // FIXME Find a way in the openmole script to call this function at the end
    Log.logger.info("Printing data...")

    for (job_id: String <- data_store.keys) {
      print_job(job_id)
    }
  }

  /**
   * Print all the informations stored about the job_id.
   * @param job_id The job to display
   */
  def print_job(job_id: String) = atomic { implicit ctx =>
    println(s"Job: $job_id")
    for (metric: String <- data_store(job_id).keys) {
      println(s"\t$metric : ${data_store(job_id)(ctx)(metric)}")
    }
  }

  /**
   * Write all the measurements of the job to the given csv file.
   * @param job_id The job to be written.
   */
  def job_csv(job_id: String) {
    Log.logger.info(s"Writing job $job_id measurements to $csv_path.")
    val file: File = new File(csv_path)

    if (!file.exists()) {
      create_csv(file)
    }

    write_job_csv(job_id, file)
  }

  /**
   * Actually write the job_id measures in the file.
   * The file _must_ be created before.
   * @param job_id The job to be written.
   * @param file The destination file.
   */
  private def write_job_csv(job_id: String, file: File) = atomic { implicit ctx =>

    file.withWriter { writer =>
      for (metric: String <- data_store(job_id).keys) {
        writer.append(data_store(job_id)(ctx)(metric).toString)
        writer.append(", ")
      }
      writer.append("\n")
    }
  }

  /**
   * Create the csv file, and then call the write_header function
   * @param file The file to be created
   */
  private def create_csv(file: File) {
    Log.logger.info(s"Creating the file $file.")
    file.getParentFile.mkdirs
    file.createNewFile

    write_header(file)
  }

  /**
   * Put the header (name of the metrics) to the given file
   * @param file The file where the header should be put
   */
  private def write_header(file: File) = atomic { implicit ctx =>
    println("Writing header")
    val writer = new BufferedWriter(new FileWriter(file, true))
    for (metric: String <- data_store(data_store.keySet.head).keys) {
      println(metric)
      writer.write(metric)
      writer.write(", ")
    }
    writer.write("\n")

    writer.close()
  }

  /**
   * Dump the data store in the given csv file.
   * File will be created if does not exist, otherwise will append to it.
   * @param path The path to the csv file
   * @return
   */
  def dump_to_csv(path: String) = {
    // TODO implement
    throw new NotImplementedError()
  }
}
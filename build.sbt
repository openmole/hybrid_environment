import scalariform.formatter.preferences._

name := "environment_listener"

version := "1.0"

scalaVersion := "2.11.7"

osgiSettings

OsgiKeys.exportPackage := Seq("environment_listener.*", "hybrid.*")

OsgiKeys.importPackage := Seq("*")

OsgiKeys.privatePackage := Seq("")

resolvers ++= 
  Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "ISC-PIF" at "http://maven.iscpif.fr/public/"
  )

val openMOLEVersion = "5.0-SNAPSHOT"

libraryDependencies += "org.openmole" %% "org-openmole-core-dsl" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-core-batch" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-tool-logger" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-plugin-task-scala" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-plugin-environment-ssh" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-plugin-environment-condor" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-plugin-environment-slurm" % openMOLEVersion

libraryDependencies += "org.openmole" %% "org-openmole-plugin-environment-egi" % openMOLEVersion

libraryDependencies += ("org.scala-stm" %% "scala-stm" % "0.7")

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
        .setPreference(IndentSpaces, 4)

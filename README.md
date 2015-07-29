# environment_listener
External openmole plugin, listening to the givens environments, and saving data and statistics about them.


## HOW TO USE IT

* COMPILE.
  * `sbt osgi-bundle`.
    * MAY NEED TO DO A `sbt publish-local` IN OPENMOLE DIRECTORY FIRST.
  * JARJAR WILL BE IN `./target/scala-2.11/` NAMED `environment_listener_2.11-1.0.jar`
* MODIFY YOUR SCRIPT.
  * ADD `import environment_listener.Listener`.
  * ADD `Listener.registerEnvironment(<environmentName>)` FOR EACH ENVIRONMENT YOU WANT TO MONITOR.
  * ADD `Listener.csv_path = "<filename>"` IF YOU DON'T WANT THE DEFAULT LOCATION
    * WHICH IS `/tmp/openmole.csv`
  * ADD `Listener.startMonitoring` ONCE YOU'RE GOOD.
* SPECIFY THE PATH TO JARJAR
  * `openmole -p <path_to_jarjar> -s <awesome_script>`
* ???
* PROFIT

![JARJAR](http://img1.wikia.nocookie.net/__cb20111208042120/starwars/images/4/42/JarJarHS-SWE.jpg)


## EXAMPLE
```Scala
import environment_listener.Listener

// Define the variables that are transmitted between the tasks
val seed = Val[Long]
val pi = Val[Double]
val piAvg = Val[Double]

STUFF

// Define the execution environment, here it is a local execution environment with 4 threads
val env = SSHEnvironment(
    "Arthur",
    "kaamelott",
    nbSlots=1
  )

Listener.registerEnvironment(env)

Listener.csv_path = "/home/Arthur/poulette.csv"

Listener.startMonitoring()

// Define and start the workflow
exploration -< (model on env hook ToStringHook()) >- (average hook ToStringHook())  
```

WILL PRODUCE
```CSV
timezone, env_kind, env_name, memory, sec, min, core, hour, day_w, day_m, month, waitingTime, execTime, totalTime, failed, 
+0100, SSHEnvironment, Arthur@kaamelott, 0, 23, 34, 4, 16, 5, 29, 7, 0, 0, 64, false, 
+0100, SSHEnvironment, Arthur@kaamelott, 0, 24, 34, 4, 16, 5, 29, 7, 0, 0, 63, false, 
+0100, SSHEnvironment, Arthur@kaamelott, 0, 24, 34, 4, 16, 5, 29, 7, 0, 0, 63, false, 
+0100, SSHEnvironment, Arthur@kaamelott, 0, 19, 35, 4, 16, 5, 29, 7, 10, 42, 52, false, 
...
```

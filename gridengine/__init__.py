"""
A lightweight Python library for distributed computing on Sun Grid Engines

GridEngine streamlines the process of managing distributed computing on a
Sun Grid Engine. It was designed by a PhD Student (me) for iterating over
algorithm and experiment design on a computing cluster.

GridEngine is best explained with some ASCII art:

          |	     JobDispatcher  ------>  Scheduler
  Host 	  |           /\                    /
          |          /  \                  /
                    /    \                /
  Comms   | ZeroMQ /      \              / Sun Grid Engine
                  /        \            /
  Cluster |     Job0  ...  Job1  ...  JobN

Jobs are wrappers around a function and its arguments. Jobs are constructed
on the host and executed on the cluster. The `JobDispatcher` is tasked with
collating and dispatching the jobs, then communicating with them once running.
The `JobDispatcher` passes the jobs to the `Scheduler` to be invoked.

Modules:
  job: The Job and JobDispatcher classes
  schedulers: `ProcessScheduler` and `GridEngineScheduler`
  functional: provides a distributed `map` function
  settings: settings that control the global behaviour of gridengine
  example: example usage of gridengine.functional.map
"""

# import the commonly used submodules for convenience
from . import functional, job, schedulers
# import the special map function into the root namespace
from functional import map

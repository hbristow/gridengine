"""A lightweight Python library for distributed computing on Sun Grid Engines

GridEngine streamlines the process of managing distributed computing on a
Sun Grid Engine. It was designed for iterating over algorithm and experiment
design on a computing cluster.

GridEngine was intentionally designed to match the API of the built-in
multiprocessing.Process and threading.Thread classes. If you have ever used
these, the gridengine.Job class will be familiar to you.

At its core, gridengine is designed to transparently schedule and execute
Jobs on a Sun Grid Engine computing cluster and return the results once the
jobs have completed. All scheduling and communication whilst jobs are running
are handled by gridengine.

The component layout of gridengine can be visualized as follows:

            |       JobDispatcher  ------>  Scheduler
    Host    |           /\                    /
            |          /  \                  /
                      /    \                /
    Comms   | ZeroMQ /      \              / Sun Grid Engine
                    /        \            /
    Cluster |     Job0  ...  Job1  ...  JobN

Jobs are wrappers around a function and its arguments. Jobs are constructed on
the host and executed on the cluster. The JobDispatcher is tasked with
collating and dispatching the jobs, then communicating with them once running.
The JobDispatcher passes the jobs to the Scheduler to be invoked.

Schedulers:
  ProcessScheduler: schedules jobs across processes on a multi-core computer
                    (laptop, etc). This is handy for debugging and experiment
                    design before scheduling thousands of jobs on the cluster
  GridEngineScheduler: schedules jobs across nodes on a Sun Grid Engine
                    (cluster). This scheduler can be used in any environment
                    which uses DRMAA, it is not strictly limited to SGE.

Once the jobs have have scheduled, they contact the JobDispatcher for their job
allocation, run the job, and submit the results back to the dispatcher before
terminating.

Modules:
  job: The Job and JobController
  dispatch: The JobDispatcher
  schedulers: ProcessScheduler, GridEngineScheduler and TimeoutError
  functional: provides a distributed 'map' function
  settings: settings that control the global behaviour of gridengine
  example: example usage of gridengine.functional.map
"""

# import commonly used names for convenience
# gridengine is not large enough to warrant module imports
# can help to stabilize the API as well (package contents can move around)
from .functional import map
from .job import Job
from .schedulers import ProcessScheduler, GridEngineScheduler, TimeoutError
from .dispatch import JobDispatcher

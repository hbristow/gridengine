GridEngine
==========
A lightweight Python library for distributed computing on Sun Grid Engines

Introduction
------------
GridEngine streamlines the process of managing distributed computing on a Sun Grid Engine. It was designed by a PhD Student (me) for iterating over algorithm and experiment design on a computing cluster.

GridEngine is best explained with some ASCII art:

            |       JobDispatcher  ------>  Scheduler
    Host    |           /\                    /
            |          /  \                  /
                      /    \                /
    Comms   | ZeroMQ /      \              / Sun Grid Engine
                    /        \            /
    Cluster |     Job0  ...  Job1  ...  JobN

Jobs are wrappers around a function and its arguments. Jobs are constructed on the host and executed on the cluster. The `JobDispatcher` is tasked with collating and dispatching the jobs, then communicating with them once running. The `JobDispatcher` passes the jobs to the `Scheduler` to be invoked.

There are two schedulers:

- `ProcessScheduler` which schedules jobs across processes on a multi-core computer (laptop, etc). This is handy for debugging and experiment design before scheduling thousands of jobs on the cluster
- `GridEngineScheduler` which schedules jobs across nodes on a Sun Grid Engine (cluster). This scheduler can actually be used in any environment which uses DRMAA.

Once the jobs have have scheduled, they contact the `JobDispatcher` for their job allocation, run the job, and submit the results back to the dispatcher before terminating.

Features
--------
 - A distributed functional `map`
 - `ProcessScheduler` and `GridEngineScheduler` schedulers for testing tasks on a laptop, then scaling them up to the Grid Engine
 - `gridengine.Job` API compatible with `threading.Thread` and `multiprocessing.Process`

Installation
------------
Get gridengine from [github](https://github.com/hbristow/gridengine):

    git clone https://github.com/hbristow/gridengine

then install it with `pip`

    cd gridengine
    pip install .

This will automatically pull and build the dependencies.

Example
-------

```python
import gridengine

def f(x):
  """compute the square of a number"""
  return x*x

scheduler = gridengine.schedulers.GridEngineScheduler()

x = [1, 2, 3, 4, 5]
y = gridengine.map(f, x, scheduler=scheduler)
```

See `gridengine/example.py` for a runnable example.

Branches/Contributing
---------------------
The dsvm branching schema is set up as follows:

    master    reflects the current latest stable version
    dev       the development track, sometimes unstable
    issue-x   bugfix branches, with issue number from GitHub
    feat-x    feature branches

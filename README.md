GridEngine
==========
A high-level Python wrapper around the Sun Grid Engine (SGE) using DRMAA and ZeroMQ

Features
--------
GridEngine streamlines the process of submitting array jobs to a Sun Grid Engine scheduler, then maintains communication between the jobs and the submission host using ZeroMQ. This allows running jobs to be monitored, killed or given updated task allocations in synchronization with other running jobs.

GridEngine was designed to distribute algorithm and experiment designs over a computing cluster.

GridEngine features:

 - A distributed functional `map`
 - `Process` and `GridEngine` schedulers for testing tasks on a laptop, then scaling them up to the Grid Engine
 - SGE Job scheduling from within Python

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

import inspect
from . import job, dispatch, schedulers

# ----------------------------------------------------------------------------
# Map
# ----------------------------------------------------------------------------
def map(f, args, scheduler=schedulers.ProcessScheduler):
  """Perform a functional-style map operation

  Apply a function f to each argument in the iterable args. This is equivalent to
    y = [f(x) for x in args]
  or
    y = map(f, args)
  except that each argument in the iterable is assigned to a separate Job
  and scheduled to run via the scheduler.

  The default scheduler is a schedulers.ProcessScheduler instance. To run map
  on a grid engine, simply pass a schedulers.GridEngineScheduler instance.

  Args:
    f: A picklable function
    args: An iterable (list) of arguments to f
    scheduler: a schedulers.Scheduler instance or class

  Returns:
    List of return values equivalent to the builtin map function

  Raises:
    Any exception that would occur when applying [f(x) for x in args]
  """

  # setup the dispatcher
  dispatcher = dispatch.JobDispatcher(scheduler)

  # allocate the jobs
  jobs = [job.Job(target=f, args=(arg,)) for arg in args]

  # run the jobs (guaranteed to return in the same order)
  dispatcher.dispatch(jobs)
  results = dispatcher.join()

  # check for exceptions
  for result in results:
    if isinstance(result, Exception):
      # an error occurred during execution of one of the jobs, reraise it
      raise result

  return results

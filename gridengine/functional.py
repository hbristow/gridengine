import inspect
import functools
from gridengine import job, dispatch, schedulers

# ----------------------------------------------------------------------------
# Partial
# ----------------------------------------------------------------------------
def isexception(x):
  """Test whether the value is an Exception instance"""
  return isinstance(x, Exception)

def isnumeric(x):
  """Test whether the value can be represented as a number"""
  try:
    float(x)
    return True
  except:
    return False


def partial(f, *args, **kwargs):
  """Return a callable partially closed over the input function and arguments

  partial is functionally equivalent to functools.partial, however it also
  applies a variant of functools.update_wrapper, with:

    __doc__    = f.__doc__
    __module__ = f.__module__
    __name__   = f.__name__ + string_representation_of_closed_arguments

  This is useful for running functions with different parameter sets, whilst
  being able to identify the variants by name
  """
  def name(var):
    try:
      return var.__name__
    except AttributeError:
      return str(var)[0:5] if isnumeric(var) else var.__class__.__name__
  g = functools.partial(f, *args, **kwargs)
  g.__doc__    = f.__doc__
  g.__module__ = f.__module__
  g.__name__   = '_'.join([f.__name__] + [name(arg) for arg in list(args)+list(kwargs.values())])
  return g


# ----------------------------------------------------------------------------
# Map
# ----------------------------------------------------------------------------
def map(f, args, scheduler=schedulers.best_available, reraise=True):
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
    f (func): A picklable function
    args (iterable): An iterable (list) of arguments to f

  Keyword Args:
    scheduler: A schedulers.Scheduler instance or class. By default, the
      system tries to return the best_available() scheduler. Use this if you
      want to set a scheduler specifically.
    reraise (bool): Reraise exceptions that occur in any of the jobs. Set this
      to False if you want to salvage any good results.

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
  if reraise:
    for exception in filter(isexception, results):
      # an error occurred during execution of one of the jobs, reraise it
      raise exception

  return results

from __future__ import print_function
import cPickle as pickle
import os
import socket
import sys
import uuid


# ----------------------------------------------------------------------------
# JOB
# ----------------------------------------------------------------------------
class Job(object):
  """
  Execution node that wrap a function, its data and return/exception values.

  A gridengine Job observes the same API as the builtin threading.Thread
  and multiprocessing.Process classes. This is to assist in transitioning
  from multiprocessing to Sun Grid Engine processing.

  Job has two use cases:
    1. Wrap a callable and its arguments in the constructor and let the grid
       engine handle everything else (the normal use case)
    2. Subclass Job and override the behaviour of the run() method

  The latter option enables more powerful use cases (such as the use of
  protocol.Protocols for bidirectional job communication), but requires more
  code.

  Note that some care has been made to ensure that Job objects and the
  functions they wrap are pickle serializable. When subclassing Job, be sure to
  do the same.

  """
  def __init__(self, target=None, name=None, args=(), kwargs={}):
    """Initialize a Job

    The constructor should always be called with keyword arguments. If
    subclassing Job, you do not need to call super().
    Args:
      target: the function to wrap. Must be serializable.
      name: a name for the job. If None, a unique identifier is generated
      args: the positional arguments to the target
      kwargs: the keyword arguments to the target
    """

    # make sure the target is a function and picklable
    if not callable(target):
      raise TypeError("'{type}' object is not callable".format(type=type(target).__name__))
    try:
      pickle.dumps(target)
    except TypeError:
      raise TypeError('lambda expressions and function objects cannot be targeted')
    if target.__module__ == '__main__':
      raise TypeError('functions in __main__ cannot be targeted')

    # store the arguments
    self.target = target
    self.name = name
    self.args = args
    self.kwargs = kwargs

  def __str__(self):
    if hasattr(self, 'target'):
      # return a truncated view of the function invocation
      f      = self.target.__name__
      args   = ['{:.5}'.format(str(arg)) for arg in self.args]
      kwargs = ['{key}={val:.8}'.format(key=key, val=str(val)) for key, val in self.kwargs.items()]
      return '{f}({args})'.format(f=f, args=', '.join([args, kwargs]))
    else:
      return object.__str__(self)

  def run(self):
    """
    Runs the Job target with the stored arguments.
    """
    result = self.target(*self.args, **self.kwargs)
    return result


# ----------------------------------------------------------------------------
# JOB CONTROLLER
# ----------------------------------------------------------------------------
class JobController(object):

  def __init__(self, submission_host, jobid):
    # actual job invocation
    import zmq
    self.context = zmq.Context()
    self.host_name = socket.gethostname()
    self.ip = socket.gethostbyname(self.host_name)
    self.transport = 'tcp://{ip}'.format(ip=self.ip)
    self.submission_host = submission_host
    self.jobid = int(jobid)

    # client/request protocol (zmq.REQ)
    self.socket = self.context.socket(zmq.REQ)
    self.socket.connect(submission_host)

  def __del__(self):
    self.socket.close()

  def start(self):
    # fetch the job
    job = self.fetch()
    # run the job
    try:
      result = job.run()
    except KeyboardInterrupt as interrupt:
      raise Exception('Job terminated with KeyboardInterrupt')
    except Exception as e:
      # store the exception to handle on the host
      result = e
    # return the result to the dispatcher controller
    self.store(result)

  def fetch(self):
    """fetch the job assignment from the job dispatcher"""
    self.socket.send(pickle.dumps({
      'jobid': self.jobid,
      'request': 'fetch_job',
      'data': None}, pickle.HIGHEST_PROTOCOL))
    return pickle.loads(self.socket.recv())

  def store(self, result):
    """Push the results back to the server"""
    self.socket.send(pickle.dumps({
      'jobid': self.jobid,
      'request': 'store_data',
      'data': result}, pickle.HIGHEST_PROTOCOL))
    return pickle.loads(self.socket.recv())

def run_from_command_line(argv):

  # retrieve the client jobid and submission host address
  script, submission_host, jobid = argv

  # create a job controller
  controller = JobController(submission_host, jobid)

  # fetch the job, run and store the results
  controller.start()

if __name__ == '__main__':
  run_from_command_line(sys.argv)

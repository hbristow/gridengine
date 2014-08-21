from __future__ import print_function
import inspect
import os
import socket
import sys
import traceback
import types
import uuid

import gridengine


# ----------------------------------------------------------------------------
# JOB
# ----------------------------------------------------------------------------
class Job(object):
  """
  Execution node that wraps a function and its arguments

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

  Note that some care has been taken to ensure that Job objects and the
  functions they wrap are gridengine.serializer serializable. When subclassing Job, be sure to
  do the same.

  """
  def __init__(self, mutable=False, target=None, name=None, args=(), kwargs={}):
    """Initialize a Job

    The constructor should always be called with keyword arguments. If
    subclassing Job, you do not need to call super().
    Args:
      mutable: reserved for subclasses. Indicates that the Job exhibits mutable
      behaviour and should be synchronized with the host on return
      target: the function to wrap. Must be serializable
      name: a name for the job. If None, a unique identifier is generated
      args: the positional arguments to the target
      kwargs: the keyword arguments to the target
    """

    # make sure the target is callable and picklable
    if not callable(target):
      raise TypeError("'{type}' object is not callable".format(type=type(target).__name__))
    try:
      gridengine.serializer.dumps(target)
    except TypeError:
      raise TypeError('Given target cannot be serialized by {s}'.format(s=gridengine.serializer.__name__))

    # store the arguments
    self.target = target
    self.name = name
    self.args = args
    self.kwargs = kwargs

    # mutation behaviour reserved for subclasses
    self.mutable = False

  def __str__(self):
    if hasattr(self, 'target'):
      # return a truncated view of the function invocation
      f      = self.target.__name__
      args   = ['{:.5}'.format(str(arg)) for arg in self.args]
      kwargs = ['{key}={val:.8}'.format(key=key, val=str(val)) for key, val in self.kwargs.items()]
      return '{f}({args})'.format(f=f, args=', '.join(args+kwargs))
    else:
      return object.__str__(self)

  def run(self, controller):
    """
    Runs the Job target with the stored arguments.

    Args:
      controller (JobController): The controller is a handle to the controller
        that is managing this job. If the target accepts 'controller' as
        a keyword argument, the controller is passed through. This allows the
        job to directly communicate with the dispatcher to, for example,
        retrieve meta job information such as jobid or push intermediate
        results back to the dispatcher
    """
    result = self.target(*self.args, **self.kwargs)
    return result


# ----------------------------------------------------------------------------
# JOB CONTROLLER
# ----------------------------------------------------------------------------
class JobController(object):

  def __init__(self, submission_host):
    # actual job invocation
    import zmq
    self.context = zmq.Context()
    self.host_name = socket.gethostname()
    self.ip = socket.gethostbyname(self.host_name)
    self.transport = 'tcp://{ip}'.format(ip=self.ip)
    self.submission_host = submission_host

    # client/request protocol (zmq.REQ)
    self.socket = self.context.socket(zmq.REQ)
    self.socket.setsockopt(zmq.RCVTIMEO, 5000)
    self.socket.connect(submission_host)

  def __del__(self):
    self.socket.close()

  def start(self):
    # fetch the job
    job = self.fetch()
    # run the job
    try:
      result = job.run(self)
    except KeyboardInterrupt as interrupt:
      raise Exception('Job terminated with KeyboardInterrupt')
    except Exception as e:
      # store the exception to handle on the host
      msg = 'Original Caller ' + traceback.format_exc()
      e.__init__(msg)
      result = e
    # return the result to the dispatcher controller
    self.store(job.id, result)

  def fetch(self):
    """fetch the job assignment from the job dispatcher"""
    self.socket.send(gridengine.serializer.dumps(
      {
        'request': 'fetch_job',
        'jobid': None,
        'data': None
      },
      gridengine.serializer.HIGHEST_PROTOCOL))
    return gridengine.serializer.loads(self.socket.recv())

  def store(self, jobid, result):
    """Push the results back to the server"""
    self.socket.send(gridengine.serializer.dumps(
      {
        'request': 'store_data',
        'jobid': jobid,
        'data': result},
      gridengine.serializer.HIGHEST_PROTOCOL))
    return gridengine.serializer.loads(self.socket.recv())

def run_from_command_line(argv):

  # retrieve the client jobid and submission host address
  script, submission_host = argv

  # create a job controller
  controller = JobController(submission_host)

  # fetch the job, run and store the results
  controller.start()

if __name__ == '__main__':
  run_from_command_line(sys.argv)

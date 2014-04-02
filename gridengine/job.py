from __future__ import print_function
import cPickle as pickle
import inspect
import os
import socket
import sys
import threading
import uuid
from . import schedulers

# ----------------------------------------------------------------------------
# JOB DISPATCHER
# ----------------------------------------------------------------------------
class JobDispatcher(object):
  """
  Server-like node tasked with dispatching and mediating jobs
  """
  def __init__(self, scheduler=schedulers.ProcessScheduler):

    # initialize the scheduler if it's not already an instance
    self.scheduler = scheduler() if inspect.isclass(scheduler) else scheduler

    # setup the ZeroMQ communications
    import zmq
    self.context = zmq.Context()
    self.host_name = socket.gethostname()
    self.ip = socket.gethostbyname(self.host_name)
    self.transport = 'tcp://{ip}'.format(ip=self.ip)

    # server/reply protocol (zmq.REP)
    self.socket = self.context.socket(zmq.REP)
    self.port = self.socket.bind_to_random_port(self.transport)
    self.address = '{transport}:{port}'.format(transport=self.transport, port=self.port)

    # poller
    self.poller = zmq.Poller()
    self.poller.register(self.socket, zmq.POLLIN)

    # control locks
    self._finished = True
    self.dispatcher_lock = threading.Lock()
    #self.scheduler_lock  = self.scheduler.lock

  def listen(self):
    print('JobDispatcher: starting job dispatcher on transport {0}'.format(self.transport))
    while not self.finished:
      # poll the socket with timeout
      if self.poller.poll(timeout=1000):
        request = pickle.loads(self.socket.recv())
        request, jobid, data = [request.get(key, None) for key in ('request', 'jobid', 'data')]
        if request == 'fetch_job':
          # find the requested job
          job = next(job for job in self.jobs if job.jobid == jobid)
          # send the job back to the client
          self.socket.send(pickle.dumps(job, pickle.HIGHEST_PROTOCOL))
        if request == 'store_data':
          # find the requested job
          job = next(job for job in self.jobs if job.jobid == jobid)
          # store the results
          job.result = data.result
          job.exception = data.exception
          self.socket.send(pickle.dumps(True, pickle.HIGHEST_PROTOCOL))

  def dispatch(self, jobs):
    # assign each of the jobs unqiue ids (1-based indexing)
    for id, job in enumerate(jobs):
      job.jobid = id + 1
    self.jobs = jobs
    # spin up the mediator
    self.finished = False
    mediator = threading.Thread(target=self.listen)
    mediator.start()
    # spin up the scheduler
    try:
      jobs = self.scheduler.schedule(self.address, jobs)
    except Exception as e:
      # catch anything so we can shut down the mediator
      print(e)
    finally:
      # shut down the mediator
      self.finished = True
      mediator.join()

    # return the transformed jobs
    return jobs

  def get_finished(self):
    with self.dispatcher_lock:
      return self._finished
  def set_finished(self, value):
    with self.dispatcher_lock:
      self._finished = value
  finished = property(get_finished, set_finished)

  def __del__(self):
    """make sure the socket is closed on deallocation"""
    if hasattr(self, 'socket'):
      self.socket.close()


# ----------------------------------------------------------------------------
# JOB
# ----------------------------------------------------------------------------
class Job(object):
  """
  Execution node that wrap a function, its data and return/exception values.
  """

  def __init__(self, submission_host=None, jobid=None):

    if submission_host:
      # actual job invocation
      import zmq
      self.context = zmq.Context()
      self.host_name = socket.gethostname()
      self.ip = socket.gethostbyname(self.host_name)
      self.transport = 'tcp://{ip}'.format(ip=self.ip)
      self.submission_host = submission_host
      self.jobid = jobid

      # client/request protocol (zmq.REQ)
      self.socket = self.context.socket(zmq.REQ)
      self.socket.connect(submission_host)

    # job template
    self.result = None
    self.exception = None

  def __del__(self):
    """make sure the socket is closed on deallocation"""
    if hasattr(self, 'socket'):
      self.socket.close()

  def __getstate__(self):
    """Select subset of attributes to pickle"""
    attributes = ['submission_host', 'jobid', 'f', 'args', 'kwargs', 'result', 'exception']
    return dict((key, val) for key, val in self.__dict__.items() if key in attributes)

  def __str__(self):
    if hasattr(self, 'f'):
      # return a truncated view of the function invocation
      return '{f}({args})'.format(f=self.f.__name__, args=', '.join(
        ['{:.5}'.format(str(arg)) for arg in self.args] +
        ['{key}={val:.8}'.format(key=str(key), val=str(val)) for key, val in self.kwargs.items()]
      ))
    else:
      # return the job spec
      return '{id} on host {host}'.format(id=self.jobid, host=self.submission_host)

  def wrap(self, f, *args, **kwargs):
    """wrap a function and its arguments to invoke"""
    self.f = f
    self.args = args
    self.kwargs = kwargs
    # return value
    self.result = None
    # exception (if one occurs)
    self.exception = None
    return self

  def fetch(self):
    """fetch the job assignment from the job dispatcher"""
    self.socket.send(pickle.dumps({
      'jobid': self.jobid,
      'request': 'fetch_job',
      'data': None}, pickle.HIGHEST_PROTOCOL))
    return pickle.loads(self.socket.recv())

  def store(self, job):
    """Push the results back to the server"""
    self.socket.send(pickle.dumps({
      'jobid': self.jobid,
      'request': 'store_data',
      'data': job}, pickle.HIGHEST_PROTOCOL))
    return pickle.loads(self.socket.recv())

  def run(self):
    """
    Runs the job function with the stored arguments.
    The return value is stored in the results attribute. If an exception is
    raised, it is suppressed and stored in the exception attribute.
    Upon collation of the results, any exceptions can be re-raised
    (as per functional.map)
    """
    try:
      self.result = self.f(*self.args, **self.kwargs)
    except KeyboardInterrupt as interrupt:
      raise interrupt
    except Exception as e:
      self.exception = e
    return self

# ----------------------------------------------------------------------------
# Job Entry Point
# ----------------------------------------------------------------------------
def run_from_command_line(argv):
  try:
    # retrieve the client jobid and submission host address
    script, submission_host, jobid = argv
    # create a new job
    job_template = Job(submission_host, int(jobid))
    job = job_template.fetch()
    job.run()
    job_template.store(job)
    return job
  except KeyboardInterrupt as interrupt:
    raise Exception('Job terminated with KeyboardInterrupt')
  except Exception as e:
    print('A gridengine exception was raised while executing job {id}. '
          'Detail: {e}'.format(id=jobid, e=e), file=sys.stderr)

if __name__ == '__main__':
  run_from_command_line(sys.argv)

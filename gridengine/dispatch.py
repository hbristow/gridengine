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
    print('JobDispatcher: starting job dispatcher on transport {0}'.format(self.address))
    while not self.finished:
      # poll the socket with timeout
      if self.poller.poll(timeout=1000):
        request = pickle.loads(self.socket.recv())
        request, jobid, data = [request.get(key, None) for key in ('request', 'jobid', 'data')]
        if request == 'fetch_job':
          # find the requested job
          job = self.job_table[jobid]
          # send the job back to the client
          self.socket.send(pickle.dumps(job, pickle.HIGHEST_PROTOCOL))
        if request == 'store_data':
          # store the results
          self.results_table[jobid] = data
          self.socket.send(pickle.dumps(True, pickle.HIGHEST_PROTOCOL))

  def dispatch(self, jobs):
    # create a shared job lookup table (1-based indexing)
    jobids = range(1, len(jobs)+1)
    self.job_table = dict((jobid, job) for jobid, job in zip(jobids, jobs))
    self.results_table = dict.fromkeys(jobids)

    # spin up the mediator
    self.finished = False
    mediator = threading.Thread(target=self.listen)
    mediator.start()
    # spin up the scheduler
    try:
      jobs = self.scheduler.schedule(self.address, self.job_table)
    except Exception as e:
      # catch anything so we can shut down the mediator
      print(e)
    finally:
      # shut down the mediator
      self.finished = True
      mediator.join()

    # return the results
    return [self.results_table[jobid] for jobid in jobids]

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

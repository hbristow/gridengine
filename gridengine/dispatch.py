from __future__ import print_function
from datetime import datetime
import inspect
import os
import socket
import sys
import threading
import traceback
import uuid

import gridengine
from gridengine import schedulers

# ----------------------------------------------------------------------------
# JOB DISPATCHER
# ----------------------------------------------------------------------------
class JobDispatcher(object):
  """
  Server-like node tasked with dispatching and mediating jobs
  """
  def __init__(self, scheduler=schedulers.best_available):
    """Initialize a new dispatcher

    Keyword Args:
      scheduler: A schedulers.Scheduler instance or class. By default, the
        system tries to return a GridEngineScheduler, and falls back to a
        ProcessScheduler if it is not available
    """

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

    # initialize the scheduler if it's not already an instance
    self.scheduler = scheduler if isinstance(scheduler, schedulers.Scheduler) else scheduler()

  def __del__(self):
    """make sure the socket is closed on deallocation"""
    self.socket.close()

  def controller(self):
    print('JobDispatcher: starting job dispatcher on transport {0}'.format(self.address))
    while not self.finished:
      # poll the socket with timeout
      if self.poller.poll(timeout=1000):
        request = gridengine.serializer.loads(self.socket.recv())
        request, jobid, data = [request.get(key, None) for key in ('request', 'jobid', 'data')]
        if request == 'fetch_job':
          # find the requested job
          job = self.job_queue.pop()
          # send the job back to the client
          self.socket.send(gridengine.serializer.dumps(job, gridengine.serializer.HIGHEST_PROTOCOL))
        if request == 'store_data':
          # store the results
          self.results[jobid] = data
          self.socket.send(gridengine.serializer.dumps(True, gridengine.serializer.HIGHEST_PROTOCOL))

  def dispatch(self, jobs):
    """Dispatch a set of jobs to run asynchronously

    Request the scheduler to schedule the set of jobs to run,
    then spin up the JobDispatcher.controller in a separate
    thread to control execution of the jobs.

    This method will raise a RuntimeError if called more than once
    before a call to join().

    Raises:
      RuntimeError: if called multiple times before a corresponding
      call to join()
    """
    if not self.finished:
      raise RuntimeError('Dispatcher is already running')

    # create a shared job lookup table (1-based indexing)
    for id, job in enumerate(jobs):
      job.id = id
    self.job_queue = [job for job in jobs]
    self.results   = dict.fromkeys(job.id for job in jobs)

    # spin up the controller
    self.finished = False
    self.job_controller = threading.Thread(target=self.controller)
    self.job_controller.start()
    # store the job start time
    self.start_time = datetime.now()
    self.end_time = None
    self.elapsed_time = None
    # spin up the scheduler
    self.scheduler.schedule(self.address, self.job_queue)

  def join(self, timeout=None):
    """Wait until the jobs terminate

    This blocks the calling thread until the jobs terminate - either
    normally or through an unhandled exception - or until the optional
    timeout occurs.

    Raises:
      TimeoutError: If the jobs have not finished before the specified timeout
      RuntimeError: If a call to join is made before dispatching
    """
    if self.finished:
      raise RuntimeError('No dispatched jobs to join')

    # raises TimeoutError
    try:
      self.scheduler.join(timeout=timeout)
    except schedulers.TimeoutError as e:
      # reraise the exception without joining the controller
      raise e
    except (KeyboardInterrupt, Exception) as e:
      # shut down the controller then reraise the exception
      self.finished = True
      self.job_controller.join()
      raise e
    else:
      # shut down the controller
      self.finished = True
      self.job_controller.join()

    # get the elapsed time
    self.end_time = datetime.now()
    self.elapsed_time = self.end_time - self.start_time
    # return the results
    return [self.results[id] for id in sorted(self.results)]

  def get_finished(self):
    with self.dispatcher_lock:
      return self._finished
  def set_finished(self, value):
    with self.dispatcher_lock:
      self._finished = value
  finished = property(get_finished, set_finished)

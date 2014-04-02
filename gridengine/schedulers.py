import os
import sys
import multiprocessing
import cPickle as pickle
from . import settings

# ----------------------------------------------------------------------------
# Generic scheduler interface
# ----------------------------------------------------------------------------
class Scheduler(object):
  """A generic scheduler interface"""
  def schedule(self, submission_host, jobs, **kwargs):
    raise NotImplementedError()


# ----------------------------------------------------------------------------
# MultiProcess Scheduler
# ----------------------------------------------------------------------------
class ProcessScheduler(Scheduler):
  """
  A Scheduler that schedules jobs as multiple processes on a multi-core CPU.
  Requires ZeroMQ, but not a Sun Grid Engine (drmaa).
  """
  def __init__(self, max_threads=None):
    # set the threads to the cpu count
    self.max_threads = max_threads if max_threads else multiprocessing.cpu_count()

  def schedule(self, submission_host, jobs, **kwargs):
    """schedule the jobs (array of job.Job) to run

    Args:
      submission_host: the address of the submission host (job.JobDispatcher.address)
      jobs: the list of job.Job items to run

    Keyword Args:
      ignored (for compatibility)
    """
    from .job import run_from_command_line
    pool = multiprocessing.Pool(processes=self.max_threads)
    print('ProcessScheduler: submitted {0} jobs across {1} concurrent processes'
          .format(len(jobs), self.max_threads))

    try:
      jobs = pool.map(run_from_command_line, [('', submission_host, j.jobid) for j in jobs])
    except Exception as e:
      pool.terminate()
      pool.join()
    else:
      pool.close()
      pool.join()
    return jobs


# ----------------------------------------------------------------------------
# Grid Engine Scheduler
# ----------------------------------------------------------------------------
class GridEngineScheduler(Scheduler):
  """
  A Scheduler that schedules jobs on a Sun Grid Engine (SGE) using the drmaa
  library
  """

  def __init__(self, **kwargs):
    import drmaa
    self.drmaa = drmaa

    # pass-through options to the jobs
    self.kwargs = kwargs
    self.whitelist = settings.WHITELIST
    self.session = drmaa.Session()
    self.session.initialize()

  def __del__(self):
    if hasattr(self, 'session'):
      self.session.exit()

  def schedule(self, submission_host, jobs, **kwargs):
    """schedule the jobs (array of job.Job) to run

    Args:
      submission_host: the address of the submission host (job.JobDispatcher.address)
      jobs: the list of job.Job items to run

    Keyword Args:
      h_cpu: maximum time expressed in format '02:00:00' (2 hours)
      h_vmem: maximum memory allocation before job is killed in format '10G' (10GB)
      virtual_free: memory free on host BEFORE job can be allocated
      swan, becks, leffe: whitelist only swan, becks or leffe machines
    """

    # build the homogenous job template and submit array
    with self.session.createJobTemplate() as jt:
      jt.jobEnvironment = os.environ

      jt.remoteCommand = os.path.expanduser(settings.WRAPPER)
      jt.args = [submission_host]
      jt.name = 'PythonGrid'
      jt.joinFiles = True
      jt.outputPath = ':'+os.path.expanduser(settings.TEMPDIR)
      jt.errorPath  = ':'+os.path.expanduser(settings.TEMPDIR)

      sgeids = self.session.runBulkJobs(jt, 1, len(jobs), 1)
      print('GridEngineScheduler: submitted {0} jobs in array {1}'
            .format(len(jobs), sgeids[0].split('.')[0]))
      for sgeid, job in zip(sgeids, jobs):
        job.sgeid = sgeid

    # wait for completion
    while True:
      try:
        self.session.synchronize(sgeids, timeout=1, dispose=True)
      except self.drmaa.ExitTimeoutException as timeout:
        # polling timed out, continue
        pass
      except KeyboardInterrupt as interrupt:
        [self.remove(job) for job in jobs]
        break
      else:
        # all jobs finished!
        break

    # collect the results
    return jobs

  def remove(self, job):
    """Remove (Terminate) a running or queued Job"""
    print('Forceably terminating job {id}'.format(id=job.sgeid))
    self.session.control(job.sgeid, self.drmaa.JobControlAction.TERMINATE)

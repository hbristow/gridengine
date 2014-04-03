import os
import sys
import multiprocessing
import cPickle as pickle
from . import job, settings

# ----------------------------------------------------------------------------
# Generic scheduler interface
# ----------------------------------------------------------------------------
class Scheduler(object):
  """A generic scheduler interface"""
  def schedule(self, submission_host, job_table, **kwargs):
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

  def schedule(self, submission_host, job_table, **kwargs):
    """schedule the jobs (dict of {jobid, job.Job}) to run

    Args:
      submission_host: the address of the submission host (job.JobDispatcher.address)
      job_table: the dict of {jobid, job.Job{ items to run

    Keyword Args:
      ignored (for compatibility)
    """
    pool = multiprocessing.Pool(processes=self.max_threads)
    print('ProcessScheduler: submitted {0} jobs across {1} concurrent processes'
          .format(len(job_table), self.max_threads))

    args = (['', submission_host, jobid] for jobid in range(1,len(job_table)+1))
    try:
      jobs = pool.map(job.run_from_command_line, args)
    except Exception as e:
      pool.terminate()
      pool.join()
    else:
      pool.close()
      pool.join()


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
    self.sgeids = []

  def __del__(self):
    try:
      # attempt cleanup
      [self.remove(sgeid) for sgeid in self.sgeids]
    except:
      pass
    self.session.exit()

  def schedule(self, submission_host, job_table, **kwargs):
    """schedule the jobs (dict of {jobid, job.Job}) to run

    Args:
      submission_host: the address of the submission host (job.JobDispatcher.address)
      job_table: the dict of {jobid, job.Job} items to run

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

      self.sgeids = self.session.runBulkJobs(jt, 1, len(job_table), 1)
      print('GridEngineScheduler: submitted {0} jobs in array {1}'
            .format(len(job_table), self.sgeids[0].split('.')[0]))

    # wait for completion
    while True:
      try:
        self.session.synchronize(self.sgeids, timeout=1, dispose=True)
      except self.drmaa.ExitTimeoutException as timeout:
        # polling timed out, continue
        pass
      except KeyboardInterrupt as interrupt:
        [self.remove(sgeid) for sgeid in self.sgeids]
        break
      else:
        # all jobs finished!
        break

  def remove(self, sgeid):
    """Remove (Terminate) a running or queued Job"""
    self.session.control(sgeid, self.drmaa.JobControlAction.TERMINATE)
    print('Forceably terminating job {id}'.format(id=sgeid))

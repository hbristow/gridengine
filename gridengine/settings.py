"""
Django-Style configuration file for GridEngine
"""
import os
import sys
import subprocess


# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
path = os.path.dirname(os.path.abspath(__file__))
PYTHONPATH = ':'.join(sys.path)
WRAPPER = os.path.join(path, 'wrapper.sh')
TEMPDIR = os.path.join(os.environ['HOME'], 'tmp')


# ----------------------------------------------------------------------------
# Advanced
# ----------------------------------------------------------------------------

# how much time can pass between heartbeats, before
# job is assummed to be dead in seconds
MAX_TIME_BETWEEN_HEARTBEATS = 90

# factor by which to increase the requested memory
# if an out of memory event was detected.
OUT_OF_MEM_INCREASE = 2.0

# defines how many times can a particular job can die,
# before we give up
NUM_RESUBMITS = 3

# check interval: how many seconds pass before we check
# on the status of a particular job in seconds
CHECK_FREQUENCY = 15

# heartbeat frequency: how many seconds pass before jobs
# on the cluster send back heart beats to the submission
# host
HEARTBEAT_FREQUENCY = 10


# ----------------------------------------------------------------------------
# White-listed grid engine computation nodes
# ----------------------------------------------------------------------------
def available_nodes():
  try:
    qstat, _  = subprocess.Popen(['qstat', '-f'], stdout=subprocess.PIPE).communicate()
    nodes     = [line.strip().split()[0] for line in qstat.splitlines() if '@' in line]
  except OSError:
    nodes = []
  return nodes

# white-list of nodes - all available nodes
WHITELIST = available_nodes()

# black-list of nodes
BLACKLIST = []

# remove black-list from white-list
for node in BLACKLIST:
    WHITELIST.remove(node)


# ----------------------------------------------------------------------------
# Display settings
# ----------------------------------------------------------------------------
if __name__ == '__main__':
  # print only the settings attributes
  for key, val in vars().items() if key.isupper():
    print(key, ' = ', val)

"""
Django-Style configuration file for GridEngine
"""
import os
import sys


# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
path = os.path.dirname(os.path.abspath(__file__))
PYTHONPATH = ':'.join(sys.path)
WRAPPER = os.path.join(path, 'wrapper.sh')
TEMPDIR = os.path.join(os.environ['HOME'], 'tmp')


# ----------------------------------------------------------------------------
# Default Resources
# ----------------------------------------------------------------------------
DEFAULT_RESOURCES = {
  'hostname': '!leffe*'
}

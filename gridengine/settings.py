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
WHITELIST = []
BLACKLIST = []

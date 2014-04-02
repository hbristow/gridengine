#!/usr/bin/env python

"""
Example usage of gridengine

Defines a function of a single argument (square) to be applied to a sequence
of arguments in parallel. The jobs can either be called locally,
or scheduled on the grid engine.

Usage:
  python -m gridengine.example
"""
from __future__ import print_function
import math
import gridengine

# The function to map across the input arguments
# Note that this function can NEVER exist in __main__
f = math.sqrt

if __name__ == '__main__':

  # the arguments
  x = [1, 2, 3, 4, 5]
  dashes = 60*'-'
  print('\n'.join([dashes, 'Inputs', dashes]))
  print('f(x) = sqrt(x)')
  print('x    =', x, end='\n\n')

  # builtin map
  dashes = 60*'-'
  print('\n'.join([dashes, 'Builtin Map', dashes]))
  y = map(f, x)
  print('y =', y, end='\n\n')

  # schedule the jobs across processes on a CPU
  print('\n'.join([dashes, 'Map Across Processes', dashes]))
  scheduler = gridengine.schedulers.ProcessScheduler()
  y = gridengine.map(f, x, scheduler=scheduler)
  print('y =', y, end='\n\n')

  # schedule the jobs on the grid engine
  print('\n'.join([dashes, 'Map Across Grid Engine', dashes]))
  scheduler = gridengine.schedulers.GridEngineScheduler()
  y = gridengine.map(f, x, scheduler=scheduler)
  print('y =', y, end='\n\n')

#!/usr/bin/env python

"""
Example usage of gridengine

Defines a function of a single argument (square) to be applied to a sequence
of arguments in parallel. The jobs can either be called locally,
or scheduled on the grid engine.

Usage:
  python -m gridengine.example
"""
import gridengine

def f(n):
  """The function to map across the input arguments"""
  return n*n

if __name__ == '__main__':

  # the arguments
  x = [1, 2, 3, 4, 5]

  # builtin map
  dashes = 60*'-'
  print('\n'.join([dashes, 'Builtin Map', dashes]))
  map(f, x)

  # schedule the jobs across processes on a CPU
  print('\n'.join([dashes, 'Map Across Processes', dashes]))
  scheduler = gridengine.schedulers.ProcessScheduler()
  gridengine.map(f, x, scheduler=scheduler)

  # schedule the jobs on the grid engine
  print('\n'.join([dashes, 'Map Across Grid Engine', dashes]))
  scheduler = gridengine.schedulers.GridEngineScheduler()
  gridengine.map(f, x, scheduler=scheduler)

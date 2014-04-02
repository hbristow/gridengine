#!/bin/bash
# GridEngine qsub submission script
# Usage:
#   python -m gridengine.job.run_from_command_line tcp://submission_host:port jobid

# get the package directory
MODULE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";
PACKAGE_DIR="$( dirname "$MODULE_DIR" )";

# append to the Python path
export PYTHONPATH=$PYTHONPATH:$PACKAGE_DIR;

# invoke the client
python -m gridengine.job $@ $SGE_TASK_ID;

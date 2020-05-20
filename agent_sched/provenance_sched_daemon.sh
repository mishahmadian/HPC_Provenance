#!/bin/bash
#|------------------------------------------------------|
#|            Scheduler Data Collector Daemon           |
#|                     Version 1.0                      |
#|                                                      |
#|       High Performance Computing Center (HPCC)       |
#|               Texas Tech University                  |
#|                                                      |
#|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
#|------------------------------------------------------|
#
# Find the Python 3.x on this system
PYTHON3=$(command -v python3)
# The minimum required version
PYTHON3_MINOR=6
# Get the current directory of the Daemon file (works when it called from somewhere else)
CURRENTPATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 || exit ; pwd -P )"
# Location of the PID file
PID_FILE=/var/run/provenance_sched.pid

# Check if Python3.x exists and has the right version
if [ -z "$PYTHON3" ]; then
  echo "Python v3.x was not found"
  exit 1
else
  # MAke sure the Python minor version meets the requirement
  if [ "$( $PYTHON3 --version | awk -F'.' '{print $2}' )" -lt $PYTHON3_MINOR ]; then
    echo "Python 3.${PYTHON3_MINOR}+ is required."
    exit 1
  fi
fi
#
# Control the Daemon:
#
case "$1" in
start)
  # Check if PID file is already exist
  if [ -f "$PID_FILE" ]; then
    if ps -p "$( cat $PID_FILE )" &> /dev/null; then
      echo "The provenance_sched is already running... pid=[$( cat $PID_FILE )]"
      exit 1
    fi
  fi
  # Start the Provenance Server Service
  $PYTHON3 "${CURRENTPATH}"/packages/schedMain.py &
  echo $! > $PID_FILE
  ;;
stop)
  # Check if PID file exist. If not then process cannot be stopped
  if [ ! -f "$PID_FILE" ]; then
    exit 1
  else
    # If PID file exist, but the pid is not running, it means the process was killed manually
    if ! ps -p "$( cat $PID_FILE )" &> /dev/null; then
      echo "The provenance_sched is not running."
      # Remove the PID file
      rm -f $PID_FILE
      exit 1
    fi
  fi
  # Send SIGINT then the main process will start wrapping gup the jobs
  kill -s SIGINT "$( cat $PID_FILE )"
  # Remove the PID file
  rm -f $PID_FILE
  ;;
restart)
  $0 stop
  $0 start
  ;;
status)
  if [ -f "$PID_FILE" ]; then
    echo "The provenance_sched is already running... pid=[$( cat $PID_FILE )]"
  else
    echo "The provenance_sched is not running."
    exit 1
  fi
  ;;
*)
  echo "[Syntax Error] $0 < start | stop | restart | status >"
esac
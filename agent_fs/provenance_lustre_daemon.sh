#!/bin/bash
#|------------------------------------------------------|
#|         Luster I/O Statistics Collector Daemon       |
#|                     Version 1.0                      |
#|                                                      |
#|       High Performance Computing Center (HPCC)       |
#|               Texas Tech University                  |
#|                                                      |
#|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
#|------------------------------------------------------|
#
# Find the Python 2.x on this system
PYTHON2=$(command -v python2)
# The minimum required version
PYTHON2_MINOR=7
# Get the current directory of the Daemon file (works when it called from somewhere else)
CURRENTPATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 || exit ; pwd -P )"
# Location of the PID file
PID_FILE=/var/run/provenance_lustre.pid

# Check if Python 2.x exists and has the right version
if [ -z "$PYTHON2" ]; then
  echo "Python v2.x was not found"
  exit 1
else
  # MAke sure the Python minor version meets the requirement
  if [ "$( $PYTHON2 --version 2>&1 | awk -F'.' '{print $2}' )" -lt $PYTHON2_MINOR ]; then
    echo "Python 2.${PYTHON2_MINOR}: is required."
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
      echo "The provenance_lustre is already running... pid=[$( cat $PID_FILE )]"
      exit 1
    fi
  fi
  # Start the Provenance Server Service
  $PYTHON2 "${CURRENTPATH}"/packages/io_collector.py &
  echo $! > $PID_FILE
  ;;
stop)
  # Check if PID file exist. If not then process cannot be stopped
  if [ ! -f "$PID_FILE" ]; then
    exit 1
  else
    # If PID file exist, but the pid is not running, it means the process was killed manually
    if ! ps -p "$( cat $PID_FILE )" &> /dev/null; then
      echo "The provenance_lustre is not running."
      # Remove the PID file
      rm -f $PID_FILE
      exit 1
    fi
  fi
  # Send SIGHUP then the main process will start wrapping gup the jobs
  kill -s SIGHUP "$( cat $PID_FILE )"
  # Remove the PID file
  rm -f $PID_FILE
  ;;
restart)
  $0 stop
  $0 start
  ;;
status)
  if [ -f "$PID_FILE" ]; then
    echo "The provenance_lustre is already running... pid=[$( cat $PID_FILE )]"
  else
    echo "The provenance_lustre is not running."
    exit 1
  fi
  ;;
*)
  echo "[Syntax Error] $0 < start | stop | restart | status >"
esac
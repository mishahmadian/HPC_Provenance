#!/bin/bash
#|------------------------------------------------------|
#|             HPC Provenance Server Daemon             |
#|                     Version 1.0                      |
#|                                                      |
#|       High Performance Computing Center (HPCC)       |
#|               Texas Tech University                  |
#|                                                      |
#|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
#|------------------------------------------------------|
#
# Cluster Name
CLUSTERS="quanah hrothgar"
# Find the Python 3.x on this system
PYTHON3=$(command -v python3)
# The minimum required version
PYTHON3_MINOR=6
# Get the current directory of the Daemon file (works when it called from somewhere else)
CURRENTPATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 || exit ; pwd -P )"
# Location of the PID file
PID_FILE=/var/run/provenance_service.pid
# Find the RabbitMQ command
RABBITMQCTL=$(command -v rabbitmqctl)
# Get the RabbitMQ virtual host name
VHOST=$(grep vhost "${CURRENTPATH}"/conf/server.conf | awk -F'=' '{print $2}')

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
# Check if RabbitMQ command exists
if [ -z "$RABBITMQCTL" ]; then
  echo "'rabbitmqctl' command was not found"
  exit1
fi
#
# Control the Daemon:
#
case "$1" in
start)
  # Check if PID file is already exist
  if [ -f "$PID_FILE" ]; then
    if ps -p "$( cat $PID_FILE )" &> /dev/null; then
      echo "The Provenance_Server is already running... pid=[$( cat $PID_FILE )]"
      exit 1
    fi
  fi
  # Clean up the RabbitMQ RPC Channel
  for CLUSTER in $CLUSTERS; do
    $RABBITMQCTL -p "$VHOST" purge_queue "${CLUSTER}"_rpc_queue
  done
  # Start the Provenance Server Service
  $PYTHON3 "${CURRENTPATH}"/packages/main_interface.py &
  echo $! > $PID_FILE
  ;;
stop)
  # Check if PID file exist. If not then process cannot be stopped
  if [ ! -f "$PID_FILE" ]; then
    exit 1
  else
    # If PID file exist, but the pid is not running, it means the process was killed manually
    if ! ps -p "$( cat $PID_FILE )" &> /dev/null; then
      echo "The Provenance_Server is not running."
      # Remove the PID file
      rm -f $PID_FILE
      exit 1
    fi
  fi
  # Send SIGHUP then the main process will start wrapping gup the jobs
  kill -s SIGHUP "$( cat $PID_FILE )"
  # Wait until the process finishes
  waitpid=0
  until [ $waitpid -eq 1 ]; do
    ps -p "$( cat $PID_FILE )" &> /dev/null
    waitpid="$?"
    sleep 1
  done
  # Remove the PID file
  rm -f $PID_FILE
  ;;
restart)
  $0 stop
  $0 start
  ;;
status)
  if [ -f "$PID_FILE" ]; then
    echo "The Provenance_Server is already running... pid=[$( cat $PID_FILE )]"
  else
    echo "The Provenance_Server is not running."
    exit 1
  fi
  ;;
*)
  echo "[Syntax Error] $0 < start | stop | restart | status >"
esac
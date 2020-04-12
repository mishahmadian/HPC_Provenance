#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
|------------------------------------------------------|
|         UGE Scheduler Data Collectore Agent        |
|                     Version 1.0                      |
|                                                      |
|       High Performance Computing Center (HPCC)       |
|               Texas Tech University                  |
|                                                      |
|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
|------------------------------------------------------|
  This process should be running on the UGE q_master
  node in order to collect Accounting data for finishe
  jobs, which cannot be collected through UGERest API
"""
from packages.schedMain import SchedMain
from daemon import DaemonContext, pidfile
import subprocess
import signal
import os
import sys

scheduler = None
context = None
pid_file = '/var/run/provenance_sched.pid'
#
# Create a Daemon Context which is going to run the agent
def start_daemon():
    # Initialize the agent
    scheduler = SchedMain()

    # Find the PID if the Daemon is running
    pid = None
    try:
        with open(pid_file) as pif:
            pid = pif.readline().strip()
    except:
        pass

    # Exit if Daemon serice is already running
    if not pid is None:
        print ("The provenance_sched_service is already running... pid=[%s]" % pid)
        sys.exit(2)

    # Ignore all the errors in stderr
    stderr = open(os.devnull, 'w')

    # Create Daemon Context if it does not exist
    context = DaemonContext(
            working_directory = os.path.dirname(os.path.realpath(__file__)),
            umask = 0o002,
            pidfile = pidfile.PIDLockFile(pid_file),
            stderr=stderr
        )

    # Map the external signals to this Daemon which will eventually
    # capture them and sends them to the agent threads
    context.signal_map = {
            signal.SIGTERM : scheduler.agent_exit,
            signal.SIGINT : scheduler.agent_exit,
        }

    # Set UId and GID
    context.uid = os.getuid()
    context.gid = os.getgid()

    # Detach the process context when opening the daemon context;
    context.detach_process = True

    # Redirect both STDOUT and STDERR to the system
    context.stdout = sys.stdout
    context.stderr = sys.stdout

    # Start the Daemon
    with context:
        scheduler.run_sched_service()

#
# Exit the Daemon and Agent gracefully
def exit_daemon(silent=True):
    # Find the PID if the Daemon is running
    pid = None
    try:
        with open(pid_file) as pif:
            pid = int(pif.readline().strip())
    except:
        pass

    # check if the service is already stopped
    if pid is None:
        print("The provenance_sched_service is not running.")
        sys.exit(2)

    # Send SIGTERM signal to the Daemon process
    os.kill(pid, signal.SIGTERM)
    #subprocess.call(f"disown {pid} && kill {pid}", shell=True)

#
# Reload the Daemon
def restart_daemon():
    exit_daemon()
    start_daemon()

#
# Show the current status
def daemon_status():
    # Find the PID if the Daemon is running
    pid = None
    try:
        with open(pid_file) as pif:
            pid = int(pif.readline().strip())
    except:
        pass

    # check if the service is already stopped
    if pid is None:
        print("The provenance_sched_service is not running.")
    else:
        print("The provenance_sched_service is running... pid=[%s]" % pid)

#
# Main
#
if __name__ == "__main__":

    # Check the command syntax
    if len(sys.argv) != 2:
        print("[Missing Argument]:  provenance_sched_agent.py < start | stop | status >")
        sys.exit(1)

    # Get the requested command
    command = sys.argv[1].strip().lower()

    # define how to switch to corresponding function
    switch = {
        'start' : start_daemon,
        'stop'  : exit_daemon,
        'restart': restart_daemon,
        'status' : daemon_status
    }

    # run the command
    try:
        switch[command]()
    except KeyError:
        print("[Wrong Argument]:  provenance_sched_agent.py < start | stop | status >")
        sys.exit(1)
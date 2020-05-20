#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
|------------------------------------------------------|
|            HPC Provenance Server Service             |
|                     Version 1.0                      |
|                                                      |
|       High Performance Computing Center (HPCC)       |
|               Texas Tech University                  |
|                                                      |
|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
|------------------------------------------------------|
  The Main Provenance Server Daemon that runs on the
  Monitoring Server machine. This service runs the
  following modules:
    - Communication Service
    - Lustre I/O Stats Modules
    - Lustre ChangeLog Module
    - Aggregator Module
    - Database Service
    - RESTful API
"""
from main_interface import Main_Interface
from daemon import DaemonContext, pidfile
import signal
import time
import os
import sys

scheduler = None
context = None
pid_file = '/var/run/provenance_service.pid'
#
# Create a Daemon Context which is going to run the agent
def start_daemon():
    # Initialize the agent
    provenance = Main_Interface()

    # Find the PID if the Daemon is running
    pid = None
    try:
        with open(pid_file) as pif:
            pid = pif.readline().strip()
    except:
        pass

    # Exit if Daemon serice is already running
    if not pid is None:
        print ("The Provenance_Server is already running... pid=[%s]" % pid)
        sys.exit(2)

    # Ignore all the errors in stderr
    #with open('output', 'w') as devnull:

    # Create Daemon Context if it does not exist
    context = DaemonContext(
            working_directory = os.path.dirname(os.path.realpath(__file__)),
            umask = 0o002,
            pidfile = pidfile.PIDLockFile(pid_file)
        )

    # Map the external signals to this Daemon which will eventually
    # capture them and sends them to the agent threads
    context.signal_map = {
            signal.SIGTERM : provenance.server_exit,
            signal.SIGINT : provenance.server_exit,
            signal.SIGHUP: provenance.server_exit,
        }

    # Set UId and GID
    context.uid = os.getuid()
    context.gid = os.getgid()

    # Detach the process context when opening the daemon context;
    context.detach_process = True

    # Redirect stdout to STDOUT and stderr to /dev/null
    context.stdout = sys.stdout
    context.stderr = open(os.devnull, 'w')

    # Start the Daemon
    with context:
        provenance.run_server()

#
# Exit the Daemon and Agent gracefully
def exit_daemon():
    # Find the PID if the Daemon is running
    pid = None
    try:
        with open(pid_file) as pif:
            pid = int(pif.readline().strip())
    except:
        pass

    # check if the service is already stopped
    if pid is None:
        print("The Provenance_Server is not running.")
        sys.exit(2)

    # Send SIGHUP signal to the Daemon process
    os.kill(pid, signal.SIGHUP)

    # Wait for the process to finish
    while True:
        try:
            os.kill(pid , 0)
        except OSError:
            break
        time.sleep(1)

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
        print("The Provenance_Server is not running.")
    else:
        print("The Provenance_Server is running... pid=[%s]" % pid)

#
# Main
#
if __name__ == "__main__":

    # Check the command syntax
    if len(sys.argv) != 2:
        print("[Missing Command]:  Provenance_Server.py < start | stop | restart | status >")
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
        print("[Wrong Command]:  Provenance_Server.py < start | stop | restart | status >")
        sys.exit(1)
    except OSError:
        print("I got it")
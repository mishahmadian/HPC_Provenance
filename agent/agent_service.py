#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
|------------------------------------------------------|
|         Luster I/O Statistics Collector Agent        |
|                     Version 1.0                      |
|                                                      |
|       High Performance Computing Center (HPCC)       |
|               Texas Tech University                  |
|                                                      |
|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
|------------------------------------------------------|
  This program has to be running on Luster MDT(s) or
  OSS(s) in order to Collect I/O operations per jobs
  which are submitted bu user to any type of resource
  scheduler (i.e PBS, Slurm, UGE/SGE, ...)
"""
from packages.io_collector import IO_Collector
from daemon import DaemonContext, pidfile
import signal
import os
import sys

ioCollector = None
context = None
pid_file = '/var/run/io_collector.pid'
#
# Create a Daemon Context which is going to run the agent
def start_daemon():
    # Initialize the agent
    ioCollector = IO_Collector()

    # Find the PID if the Daemon is running
    pid = None
    try:
        with open(pid_file) as pif:
            pid = pif.readline().strip()
    except:
        pass

    # Exit if Daemon serice is already running
    if pid != None:
        print "The agent_service is already running... pid=[%s]" % pid
        sys.exit(2)

    # Create Deamon Context if it does not exist
    context = DaemonContext(
            working_directory = os.path.dirname(os.path.realpath(__file__)),
            umask = 0o002,
            pidfile = pidfile.PIDLockFile(pid_file)
        )

    # Map the external signals to this Daemon which will eventually
    # capture them and sends them to the agent threads
    context.signal_map = {
            signal.SIGTERM : ioCollector.agent_exit,
            signal.SIGINT : ioCollector.agent_exit,
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
        ioCollector.agent_run()

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
    if pid == None:
        print "The agent_service is not running."
        sys.exit(2)

    # Send SIGTERM signal to the Daemon process
    os.kill(pid, signal.SIGTERM)

#
# Reload the Daemon
def reload_daemon():
    pass

#
# Main
#
if __name__ == "__main__":

    # Check the command syntax
    if len(sys.argv) != 2:
        print "[Missing Argument]:  agent_service.py < start | stop | reload >"
        sys.exit(1)

    # Get the requested command
    command = sys.argv[1].strip().lower()

    # define how to swith to corresponding function
    switch = {
        'start' : start_daemon,
        'stop'  : exit_daemon,
        'reload': reload_daemon
    }

    # run the command
    try:
        switch[command]()
    except KeyError:
        print "[Wrong Argument]:  agent_service.py < start | stop | reload >"
        sys.exit(1)

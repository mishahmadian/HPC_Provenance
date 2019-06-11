#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
|------------------------------------------------------|
|          Luster JobSTAT Monitoring Agent             |
|                     Version 1.0                      |
|                                                      |
|       High Performance Computing Center (HPCC)       |
|               Texas Tech University                  |
|                                                      |
|       Misha Ahmadian (misha.ahmadian@ttu.edu)        |
|------------------------------------------------------|
  This Code has to be running on Luster MDT(s) or
  OSS(s) in order to Collect I/O operations per jobs
  which are submitted bu user to any type of resource
  scheduler (i.e PBS, Slurm, UGE/SGE, ...)
"""

from packages.Config import AgentConfig, ConfigReadExcetion
from packages.Communication import Producer, CommunicationExp
from threading import Thread, Event
from Queue import Queue
import subprocess
import time
import sys
import signal
import socket
import json


#
#  Defined a Class for collecting Jobstats on MDS(s) and OSS(s)
#
class CollectJobstats(Thread):
    def __init__(self, jobstat_Q):
        Thread.__init__(self)
        self.exit_flag = Event()
        self.config = AgentConfig()
        self.jobstat_Q = jobstat_Q
        self.hostname = socket.gethostname()
        # Set Jobstat cleanup interval
        self.__setMaxAutoCleanup(self.config.getMaxJobstatAge())

    # Extends Thread.run()
    def run(self):
        while not self.exit_flag.is_set():
            try:
                serverParam = self.__getServerParam()
                # Collecting Jobstats from Lustre
                jobstat_out = self.__getJobStats(serverParam)
                # Put the jobstat output in thread safe Queue
                if jobstat_out.strip():
                    self.jobstat_Q.put(jobstat_out)
                    # Clear JobStats logs immediately to free space
                    self.__clearJobStats(serverParam)
                #
                waitInterval = self.config.getJobstatsInterval()
                self.exit_flag.wait(waitInterval)

            except ConfigReadExcetion as confExp:
                print confExp.getMessage()
                self.exit_flag.set()

    # Load the Agent Settings from Agent.conf file
    def __getServerParam(self):
        if self.hostname in self.config.getMDS_hosts():
            return "mdt"
        elif self.hostname in self.config.getOSS_hosts():
            return "obdfilter"
        else:
            raise ConfigReadExcetion("This hostname is not valid . Please check the hostname in 'agent.config' file")

    # Read the Jobstats from Lustre logs
    def __getJobStats(self, serverParam):
        return subprocess.check_output("lctl get_param " + serverParam + ".*.job_stats | tail -n +2", shell=True)

    # Clear the accumulated JobStats from Luster logs
    def __clearJobStats(self, serverParam):
        subprocess.check_output("lctl set_param " + serverParam + ".*.job_stats=clear", shell=True)

    # Set the Maximum auto-cleanup Interval for jobstats
    def __setMaxAutoCleanup(self, interval):
        subprocess.check_output("lctl set_param *.*.job_cleanup_interval=" + interval, shell=True)


#
#  Defined Class for Sending/Publishing Jobstats to Monitoring Server program
#
class PublishJobstats(Thread):
    def __init__(self, jobstat_Q):
        Thread.__init__(self)
        self.exit_flag = Event()
        self.jobstat_Q = jobstat_Q
        self.hostname = socket.gethostname()

        try:
            self.producer = Producer()

        except CommunicationExp:
            raise

    def run(self):
        while not self.exit_flag.is_set():
            if not self.jobstat_Q.empty():
                jobstat_msg = self.jobstat_Q.get()
                timestamp = time.time()
                message_body = {'server' : self.hostname,
                                'timestamp' : timestamp,
                                'output': jobstat_msg}

                # Convert Message body dictionary to JSON format
                message_json = json.dumps(message_body)

                # Send the message to Server
                try:
                    self.producer.send(message_json)

                except CommunicationExp as commExp:
                    print commExp.getMessage()
                    self.exit_flag.set()

            #
            sendingInterval = self.producer.getInterval()
            self.exit_flag.wait(sendingInterval)

#
#  Exception will be raised when SIGINT or SIGTERM are called
#
class MonitoringExitExp(Exception):
    pass

# Handle the SIGINT and SIGTERM signals in order to shutdown
# the Collector agent
#
def agent_exit(sig, frame):
    raise MonitoringExitExp

# Main Function
def main():
    collJstat_Thr = None
    pubJstat_Thr = None
    # Register signal handler
    signal.signal(signal.SIGINT, agent_exit)
    signal.signal(signal.SIGTERM, agent_exit)

    try:
        jobstat_Q = Queue()

        # Jobstat Collection thread
        collJstat_Thr = CollectJobstats(jobstat_Q)
        collJstat_Thr.start()

        # JobStat producer thread
        pubJstat_Thr = PublishJobstats(jobstat_Q)
        pubJstat_Thr.start()

        # Keep the main thread running to catch signals
        while True:
            time.sleep(0.5)
            if not collJstat_Thr.isAlive() or not pubJstat_Thr.isAlive():
                raise MonitoringExitExp

    except MonitoringExitExp:
        print ("\nAgent is shutting down..."),

    except ConfigReadExcetion as confExp:
        print confExp.getMessage()

    except CommunicationExp as commExp:
        print commExp.getMessage()

    except Exception as exp:
        print str(exp)

    finally:
            try:
                if not collJstat_Thr == None:
                    collJstat_Thr.exit_flag.set()
                    collJstat_Thr.join()
                if not pubJstat_Thr == None:
                    pubJstat_Thr.exit_flag.set()
                    pubJstat_Thr.join()

                print "Done!"

            except:
                pass
#
# Main
#
if __name__ == "__main__":
    main()

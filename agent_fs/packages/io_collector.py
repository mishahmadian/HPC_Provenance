# -*- coding: utf-8 -*-
"""
    The main  module which collects the I/O statistics from lustre
    Servers such as MDS(s) and OSS(s). This module spawn three Threads:

        1. CollectIOStats: Collecting the I/O statistics from lustre within
           the specified time intervals and store it in a Queue

        2. Pick collected I/O stats from the Queue and publish (send) them
           to the server which for further procees.

        3. IO_Collector which is the main thread to be used in Agent Deamon


 Misha ahmadian (misha.ahmadian@ttu.edu)
"""

from Config import AgentConfig, ConfigReadExcetion
from Communication import Producer, CommunicationExp
from threading import Thread, Event
from ntplib import NTPClient
from Queue import Queue
import subprocess
import signal
import socket
import json
import time

#
#  Defined a Class for collecting JobStats on MDS(s) and OSS(s)
#
class CollectIOstats(Thread):
    def __init__(self, jobstat_Q):
        Thread.__init__(self)
        self.exit_flag = Event()
        self.config = AgentConfig()
        self.jobstat_Q = jobstat_Q
        self.hostname = socket.gethostname()
        # Set JobStat cleanup interval
        if self.hostname in self.config.getMDS_hosts():
            self.__setMaxAutoCleanup(self.config.getMaxJobstatAge())

    # Implement Thread.run()
    def run(self):
        while not self.exit_flag.is_set():
            try:
                serverParam = self.__getServerParam()
                # Collecting JobStats from Lustre
                jobstat_out = self.__getJobStats(serverParam)
                # Put the jobStat output in thread safe Queue
                if jobstat_out.strip():
                    self.jobstat_Q.put(jobstat_out)
                    # Clear JobStats logs immediately to free space
                    ##-- self.__clearJobStats(serverParam)

                # Set time interval for Collecting IO stats
                waitInterval = self.config.getJobstatsInterval()
                self.exit_flag.wait(waitInterval)

            except ConfigReadExcetion as confExp:
                print(confExp.getMessage())
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
class PublishIOstats(Thread):
    def __init__(self, jobstat_Q):
        Thread.__init__(self)
        self.exit_flag = Event()
        self.jobstat_Q = jobstat_Q
        self.hostname = socket.gethostname()
        self.config = AgentConfig()

        try:
            self.producer = Producer()

        except CommunicationExp:
            raise


    def run(self):
        while not self.exit_flag.is_set():
            if not self.jobstat_Q.empty():
                jobstat_msg = self.jobstat_Q.get()
                timestamp = time.time()
                message_body = {"server" : self.hostname,
                                "timestamp" : timestamp,
                                "output": jobstat_msg}

                # Convert Message body dictionary to JSON format
                message_json = json.dumps(message_body)

                # Send the message to Server
                try:
                    self.producer.send(message_json)

                except CommunicationExp as commExp:
                    print(commExp.getMessage())
                    self.exit_flag.set()

            #
            sendingInterval = self.producer.getInterval()
            self.exit_flag.wait(sendingInterval)

    # ------INCOMPLETE--------
    # The NTP option can be add in the future, but the machine time
    # will be used for timestamp assuming that the machine is connected
    # to a reliable NTP server
    def __get_ntp_time(self):
        ntp = NTPClient()
        ntp_server = None #self.config.getNTPServer()
        try:
            response = ntp.request(ntp_server)
            return response.tx_time
        except NTPException as ntpExp:
            print(str(ntpExp))
            self.exit_flag.set()



#
#  Exception will be raised when SIGINT or SIGTERM are called
#
class MonitoringExitExp(Exception):
    pass

#
# The main class which generates the Lustre statistic collector thread
# and communication channel thread
# this class is executed by the main Daemon process
#
class IO_Collector:
    def __init__(self):
        self.IOStats_Thr = None
        self.pubJstat_Thr = None

        # Register signal handler
        signal.signal(signal.SIGINT, self.agent_exit)
        signal.signal(signal.SIGTERM, self.agent_exit)

    # Handle the SIGINT and SIGTERM signals in order to shutdown
    # the Collector agent
    def agent_exit(self, sig, frame):
        raise MonitoringExitExp

    # Main Function
    def agent_run(self):
        try:
            jobstat_Q = Queue()

            # Jobstat Collection thread
            self.IOStats_Thr = CollectIOstats(jobstat_Q)
            self.IOStats_Thr.start()

            # JobStat producer thread
            self.pubJstat_Thr = PublishIOstats(jobstat_Q)
            self.pubJstat_Thr.start()

            #print "\nProvenance FS agent has been restarted."

            # Keep the main thread running to catch signals
            while True:
                time.sleep(0.5)
                if not self.IOStats_Thr.isAlive() or not self.pubJstat_Thr.isAlive():
                    raise MonitoringExitExp

        except MonitoringExitExp:
            print("\nProvenance FS agent is shutting down..."),

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

        except CommunicationExp as commExp:
            print(commExp.getMessage())

        except Exception as exp:
            print(str(exp))

        finally:
                try:
                    if not self.IOStats_Thr is None:
                        self.IOStats_Thr.exit_flag.set()
                        self.IOStats_Thr.join()
                    if not self.pubJstat_Thr is None:
                        self.pubJstat_Thr.exit_flag.set()
                        self.pubJstat_Thr.join()

                    print("Done!")

                except:
                    pass

#
# Main
#
if __name__ == "__main__":
    ioCollector = IO_Collector()
    ioCollector.agent_run()

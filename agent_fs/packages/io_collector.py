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
from Logger import log, Mode
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
            if self._is_MGS():
              self._setMaxAutoCleanup(self.config.getMaxJobstatAge())

    # Implement Thread.run()
    def run(self):
        # Is this server MDS or OSS?
        serverParam = self._getServerParam()
        # What are MDT(s) or OST(s)
        fsnames = self._getfsnames(serverParam)

        while not self.exit_flag.is_set():
            try:
                for target in fsnames:
                    # Collecting JobStats from Lustre
                    jobstat_out = self._getJobStats(serverParam, target)
                    # Put the jobStat output in thread safe Queue along with the fsname target
                    if jobstat_out.strip():
                        self.jobstat_Q.put((target, jobstat_out))
                        # Clear JobStats logs immediately to free space
                        ##-- self._clearJobStats(serverParam)

                # Set time interval for Collecting IO stats
                waitInterval = self.config.getJobstatsInterval()
                self.exit_flag.wait(waitInterval)

            except ConfigReadExcetion as confExp:
                log(Mode.IO_COLLECTOR, confExp.getMessage())
                self.exit_flag.set()

    # Load the Agent Settings from Agent.conf file
    def _getServerParam(self):
        if self.hostname in self.config.getMDS_hosts():
            return "mdt"
        elif self.hostname in self.config.getOSS_hosts():
            return "obdfilter"
        else:
            raise ConfigReadExcetion("This hostname is not valid . Please check the hostname in 'agent.config' file")

    # Find the existing file system names to address the MDT and OST targets
    def _getfsnames(self, serverParam):
        fsname = subprocess.check_output("lctl list_param " + serverParam + ".*" , shell=True)
        fsnames = fsname.encode(encoding='UTF=8').strip().split('\n')
        for inx, val in enumerate(fsnames):
            fsnames[inx] = val.split('.')[1].strip()
        return fsnames

    # Read the Jobstats from Lustre logs
    def _getJobStats(self, serverParam, fsname):
        param = '.'.join([serverParam, fsname, 'job_stats'])
        return subprocess.check_output("lctl get_param " + param + " | tail -n +2", shell=True)

    # Check and see if the server is MGS (Lustre Management Server)
    def _is_MGS(self):
        mgs = subprocess.check_output("lctl dl | grep -i mgs" , shell=True, encoding="UTF-8")
        if mgs.strip(): return True
        return False

    # Clear the accumulated JobStats from Luster logs
    def _clearJobStats(self, serverParam):
        subprocess.check_output("lctl set_param " + serverParam + ".*.job_stats=clear", shell=True)

    # Set the Maximum auto-cleanup Interval for jobstats
    def _setMaxAutoCleanup(self, interval):
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
                                "fstarget" : jobstat_msg[0],
                                "output": jobstat_msg[1]}

                # Convert Message body dictionary to JSON format
                message_json = json.dumps(message_body)

                # Send the message to Server
                try:
                    self.producer.send(message_json)

                except CommunicationExp as commExp:
                    log(Mode.IO_COLLECTOR, commExp.getMessage())
                    self.exit_flag.set()

            #
            sendingInterval = self.producer.getInterval()
            self.exit_flag.wait(sendingInterval)


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
            log(Mode.APP_EXIT, "***************** Provenance Lustre Agent Started *****************")

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
            log(Mode.APP_EXIT, "***************** Provenance Lustre Agent Stopped *****************")
            try:
                if not self.IOStats_Thr is None:
                    self.IOStats_Thr.exit_flag.set()
                    self.IOStats_Thr.join()
                if not self.pubJstat_Thr is None:
                    self.pubJstat_Thr.exit_flag.set()
                    self.pubJstat_Thr.join()

            except Exception as exp:
                log(Mode.IO_COLLECTOR, "[Error on Exit]" + str(exp))

        except ConfigReadExcetion as confExp:
            log(Mode.IO_COLLECTOR, confExp.getMessage())

        except CommunicationExp as commExp:
            log(Mode.IO_COLLECTOR, commExp.getMessage())

        except Exception as exp:
            print(str(exp))


#
# Main
#
if __name__ == "__main__":
    ioCollector = IO_Collector()
    ioCollector.agent_run()

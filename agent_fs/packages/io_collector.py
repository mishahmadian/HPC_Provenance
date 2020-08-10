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

from subprocess import check_output, Popen, PIPE, STDOUT, CalledProcessError
from Communication import Producer, CommunicationExp
from Config import AgentConfig, ConfigReadExcetion
from multiprocessing import Process, Event
from Logger import log, Mode
import signal
import socket
import json
import time
import os
from pprint import pprint

#
#  Defined a Class for collecting JobStats on MDS(s) and OSS(s)
#
class CollectIOstats(Process):
    def __init__(self, shutdown):
        Process.__init__(self)
        self.config = AgentConfig()
        self.shutdown = shutdown
        self.hostname = socket.gethostname()
        try:
            # Set time interval for Collecting IO stats
            self._waitInterval = self.config.getJobstatsInterval()

            self.maxJobStatAge = self.config.getMaxJobstatAge()
            self.cpu_load = self.config.is_CPU_Load_avail()
            self.mem_usage = self.config.is_Mem_Usage_avail()
            # Create producer communication
            self.producer = Producer()

            ## --if self.hostname in self.config.getMDS_hosts():
            # Set JobStat cleanup interval
            # In case that Lustre was down, it waits
            while True:
                output = self._setMaxAutoCleanup(self.config.getMaxJobstatAge())
                if "error" not in output.decode(encoding='UTF=8'):
                    break
                time.sleep(self._waitInterval)

        except ConfigReadExcetion as confExp:
            log(Mode.IO_COLLECTOR, confExp.getMessage())

        except CommunicationExp as commExp:
            log(Mode.PUBLISHER, commExp.getMessage())

    def run(self):
        while not self.shutdown.is_set():
            try:
                # Is this server MDS or OSS?
                serverParam = self._getServerParam()
                # What are MDT(s) or OST(s)
                fsnames = self._getfsnames(serverParam)

                # Continue if lustre is up and there is no error
                if isinstance(fsnames, list) and "error" not in fsnames:
                    for target in fsnames:
                        # Collecting JobStats from Lustre
                        jobstat_out = self._getJobStats(serverParam, target)
                        # Break if error happens due to lustre issue
                        if "error" in jobstat_out.decode(encoding='UTF=8'):
                            log(Mode.IO_COLLECTOR, jobstat_out.decode(encoding='UTF=8'))
                            break

                        # Put the jobStat output in thread safe Queue along with the fsname target
                        if jobstat_out.strip():
                            self._publish_jobstats(jobstat_out, target)
                            # Clear JobStats logs immediately to free space
                            ##--self._clearJobStats(serverParam, target)
                else:
                    log(Mode.IO_COLLECTOR, fsnames)

                self.shutdown.wait(self._waitInterval)

            except ConfigReadExcetion as confExp:
                log(Mode.IO_COLLECTOR, confExp.getMessage())
                self.shutdown.set()

            except MonitoringExitExp:
                pass


    # Load the Agent Settings from Agent.conf file
    def _getServerParam(self):
        if self.hostname in self.config.getMDS_hosts():
            return "mdt"
        elif self.hostname in self.config.getOSS_hosts():
            return "obdfilter"
        else:
            raise ConfigReadExcetion("This hostname is not valid . Please check the hostname in 'agent.config' file")

    # Find the existing file system names to address the MDT and OST targets
    @classmethod
    def _getfsnames(cls, serverParam):
        fsname = cls._lustre_command("lctl list_param " + serverParam + ".*")
        fsname = fsname.decode(encoding='UTF=8').strip()
        # Return the error when error occurs
        if "error" in fsname:
            return fsname

        fsnames = fsname.split('\n')
        for inx, val in enumerate(fsnames):
            fsnames[inx] = val.split('.')[1].strip()
        return fsnames

    # Read the Jobstats from Lustre logs
    @classmethod
    def _getJobStats(cls, serverParam, fsname):
        param = '.'.join([serverParam, fsname, 'job_stats'])
        read_jobstat = "lctl get_param " + param + " | tail -n +2 "
        clear_jobstat = " lctl set_param " + param + "=clear &>/dev/null"
        return cls._lustre_command('&&'.join([read_jobstat, clear_jobstat]))
        # param = '.'.join([serverParam, fsname, 'job_stats'])
        # return subprocess.check_output("lctl get_param " + param + " | tail -n +2", shell=True)

    # Check and see if the server is MGS (Lustre Management Server)
    @classmethod
    def _is_MGS(cls):
        mgs = cls._lustre_command("lctl dl | grep -i mgs")
        if mgs.decode("utf-8").strip(): return True
        return False

    # Clear the accumulated JobStats from Luster logs
    @classmethod
    def _clearJobStats(cls, serverParam, fsname):
        return cls._lustre_command("lctl set_param " + serverParam + "." + fsname +".job_stats=clear")

    # Set the Maximum auto-cleanup Interval for jobstats
    @classmethod
    def _setMaxAutoCleanup(cls, interval):
        return cls._lustre_command("lctl set_param *.*.job_cleanup_interval=" + interval)

    # The man subprocess call
    @staticmethod
    def _lustre_command(cmd):
        try:
            return check_output(cmd, shell=True, stderr=STDOUT)
        except CalledProcessError as callexp:
            log(Mode.IO_COLLECTOR, str(callexp))
            return "error: " + str(callexp)

    @staticmethod
    def _getServerLoadAvg():
        try:
            return os.getloadavg()

        except OSError:
            return 0.0, 0.0, 0.0

    @staticmethod
    def _getServerMemoryUsage():
        try:
            out, err = Popen(['free', '-t', '-m'], stdout=PIPE).communicate()
            if not err and out:
                return map(int, out.splitlines()[-1].split()[1:3])
            else:
                return [0, 0]
        except Exception:
            return [0, 0]

    # Send the job stats data along with server stats to
    # the provenance server
    def _publish_jobstats(self, jobstat_data, fstarget):

        timestamp = time.time()
        message_body = {"server": self.hostname,
                        "timestamp": timestamp,
                        "maxAge": self.maxJobStatAge,
                        "fstarget": fstarget,
                        "output": jobstat_data}

        if self.cpu_load:
            message_body.update({"serverLoad": self._getServerLoadAvg()})

        if self.mem_usage:
            message_body.update({"serverMemory": self._getServerMemoryUsage()})

        # Convert Message body dictionary to JSON format
        message_json = json.dumps(message_body)

        # Send the message to Server
        try:
            self.producer.send(message_json)

        except CommunicationExp as commExp:
            log(Mode.IO_COLLECTOR, commExp.getMessage())
            self.shutdown.set()


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
        self.iostats_proc = None
        self.shutdown = Event()

        # Register signal handler
        signal.signal(signal.SIGINT, self.agent_exit)
        signal.signal(signal.SIGTERM, self.agent_exit)
        signal.signal(signal.SIGHUP, self.agent_exit)

    # Handle the SIGINT and SIGTERM signals in order to shutdown
    # the Collector agent
    def agent_exit(self, sig, frame):
        raise MonitoringExitExp

    # Main Function
    def agent_run(self):
        try:
            log(Mode.APP_START, "***************** Provenance Lustre Agent Started *****************")

            # Jobstat Collection thread
            self.iostats_proc = CollectIOstats(self.shutdown)
            self.iostats_proc.start()

            # Keep the main thread running to catch signals
            while True:
                time.sleep(0.5)
                if not self.iostats_proc.is_alive():
                    raise MonitoringExitExp

        except MonitoringExitExp:
            log(Mode.APP_EXIT, "***************** Provenance Lustre Agent Stopped *****************")
            self.shutdown.set()
            try:
                if self.iostats_proc and self.iostats_proc.is_alive():
                    time.sleep(2)
                    self.iostats_proc.terminate()
                    self.iostats_proc.join()
                print("I'm finished")

            except Exception as exp:
                log(Mode.IO_COLLECTOR, "[Error on Exit]" + str(exp))

        except ConfigReadExcetion as confExp:
            log(Mode.IO_COLLECTOR, confExp.getMessage())

        except CommunicationExp as commExp:
            log(Mode.IO_COLLECTOR, commExp.getMessage())

        except Exception as exp:
            log(Mode.IO_COLLECTOR, str(exp))


#
# Main
#
if __name__ == "__main__":
    ioCollector = IO_Collector()
    ioCollector.agent_run()

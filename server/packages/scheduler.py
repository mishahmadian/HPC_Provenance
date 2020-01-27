# -*- coding: utf-8 -*-
"""
    The Scheduler package talks to the various type of available Job schedulers and collects
    necessary job information from different clusters

        - By now it only works with Univa Grid Engine (UGE)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
from enum import Enum, unique
import uge_service
import hashlib

class JobScheduler:
    def __init__(self):
        self.config = ServerConfig()
        # list of all available schedulers
        self.__schedulers = {
            'uge' : self.__get_UGE_JobInfo
        }
    #
    # The main method that gets the jobInfo object regardless of scheduler type and the type of job
    #
    def getJobInfo(self, cluster, sched, jobId, taskid) -> 'JobInfo':
        # Check if scheduler type is already supported
        if sched not in self.__schedulers.keys():
            raise JobSchedulerException("The Scheduler type [" + sched + "] is invalid")

        # Call the proper method based on the scheduler type
        return self.__schedulers[sched](cluster, sched, jobId, taskid)


    #
    # This method Collects Job Info from Univa Grid Engine (UGE) job scheduler by:
    #   1. using UGE Restful API for running jobs
    #   2. UGE accounting data for those jobs that are already finished
    #
    #   *** We assume both UGERest and UGE Accounting are properly running on q_master node
    #
    def __get_UGE_JobInfo(self, cluster, sched, jobId, taskid) -> 'UGEJobInfo':
        uge_clusters = self.config.getUGE_clusters()
        if cluster in uge_clusters:
            cluster_inx = uge_clusters.index(cluster)
        else:
            raise JobSchedulerException("Configurations specs for [" + cluster + "] cannot be found in server.conf")

        uge_ip = self.config.getUGE_Addr()[cluster_inx]
        uge_port = self.config.getUGE_Port()[cluster_inx]

        # Call UGERestful API to collect Job Info
        jobInfo = uge_service.UGERest.getUGEJobInfo(uge_ip, uge_port, jobId, taskid)

        # return if jobInfo could be collected successful from UGERest
        if jobInfo:
            # Complete the Object
            jobInfo.cluster = cluster
            jobInfo.sched_type = sched
            return jobInfo

        # Otherwise, Look into UGE Accounting for Job Info



# The Super class for all type of JobInfo objects which contains the necessary attributes
class JobInfo(object):
    def __init__(self):
        self.jobid = None
        self.cluster = None
        self.sched_type = None
        self.taskid = None
        self.status = self.Status.NONE
        self.jobName = None
        self.queue = None
        self.num_cpu = None
        self.submit_time = None
        self.start_time = None
        self.end_time = None
        self.username = None

    # This function returns a unique ID for every objects with the same JobID, Scheduler, and cluster
    def uniqID(self):
        # calculate the MD5 hash
        obj_id = ''.join([self.sched_type, self.cluster, self.jobid])
        hash_id = hashlib.md5(obj_id.encode(encoding='UTF=8'))
        return hash_id.hexdigest()
    #
    # Defines the Status of the current Job
    #
    @unique
    class Status(Enum):
        NONE = 0
        RUNNING = 1
        QWAITING = 2
        DELETING = 3
        ERROR = 4
        OTHERS = 5
        FINISHED = 6



# Sub class of JobInfo specifically for Univa Grid Engine (UGE) scheduler which contains
# some extra attributes that may not be available in other schedulers or have different type.
class UGEJobInfo(JobInfo):
    def __init__(self):
        JobInfo.__init__(self)
        self.h_rt = None
        self.s_rt = None
        self.h_vmem = None
        self.parallelEnv = None
        self.project = None
        self.pwd = None
        self.command = None
        self.cpu = None
        self.io = None
        self.ioops = None
        self.iow = None
        self.maxvmem = None
        self.mem = None
        self.wallclock = None

#
# In any case of Error, Exception, or Mistake JobSchedulerException will be raised
#
class JobSchedulerException(Exception):
    def __init__(self, message):
        super(JobSchedulerException, self).__init__(message)
        self.message = "\n [Error] _JOB_SCHED_: " + message + "\n"

    def getMessage(self):
        return self.message
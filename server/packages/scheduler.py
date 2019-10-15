# -*- coding: utf-8 -*-
"""
    The Scheduler package talks to the various type of available Job schedulers and collects
    necessary job information from different clusters

        - By now it only works with Univa Grid Engine (UGE)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
import hashlib

class JobScheduler:
    def __init__(self):
        self.config = ServerConfig()
        try:
            # Get the list of all available schedulers
            self.__schedulerList = self.config.getSchedulersList()

            for sched in self.__schedulerList:
                # Check and see if the scheduler type defined in server.conf is valid
                # More supported schedulers can added here later
                if sched != "uge":
                    raise JobSchedulerExcetion("The Scheduler type [" + sched + "] is invalid")
            
        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

    def getJobInfo(self, cluster, scheduler, jobId):
        pass

# The Super class for all type of JobInfo objects which contains the necessary attributes
class JobInfo(object):
    def __init__(self):
        self.jobid = None
        self.cluster = None
        self.sched_type = None
        self.name = None
        self.queue = None
        self.num_cpu = None
        self.start_time = None
        self.end_time = None
        self.username = None

    # This function returns a unique ID for every objects with the same JobID, Scheduler, and cluster
    def uniqID(self):
        # calculate the MD5 hash
        obj_id = ''.join([self.sched_type, self.cluster, self.jobid])
        hash_id = hashlib.md5(obj_id.encode(encoding='UTF=8'))
        return hash_id.hexdigest()


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

#
# In any case of Error, Exception, or Mistake JobSchedulerExcetion will be raised
#
class JobSchedulerExcetion(Exception):
    def __init__(self, message):
        super(JobSchedulerExcetion, self).__init__(message)
        self.message = "\n [Error] _JOB_SCHED_: " + message + "\n"

    def getMessage(self):
        return self.message
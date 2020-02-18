# -*- coding: utf-8 -*-
"""
    The Scheduler package talks to the various type of available Job schedulers and collects
    necessary job information from different clusters

        - By now it only works with Univa Grid Engine (UGE)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
from persistant import FinishedJobs
from enum import Enum, unique
import uge_service
import hashlib

class JobScheduler:
    def __init__(self):
        self.config = ServerConfig()
        # list of all supported schedulers
        self.__schedulers = {
            'uge' : self.__get_UGE_JobInfo
        }

        # -------------------------------------------------------------------------------------
        # Following section executes when [uge] section appears in server.conf
        #
        self.__ugeService = None
        if self.config.getUGE_clusters():
            self.__ugeService = uge_service.UGE()
        # -------------------------------------------------------------------------------------
    #
    # The main method that gets the jobInfo object regardless of scheduler type and the type of job
    #
    def getJobInfo(self, cluster, sched, jobId, taskid) -> 'JobInfo':
        # Check if scheduler type is already supported
        if sched not in self.__schedulers.keys():
            raise JobSchedulerException("The Scheduler type [" + sched + "] is not supported")

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
        # The requested cluster must exist in server.conf
        uge_clusters = self.config.getUGE_clusters()
        if cluster in uge_clusters:
            cluster_inx = uge_clusters.index(cluster)
        else:
            raise JobSchedulerException("Configurations specs for [" + cluster + "] cannot be found in server.conf")

        # -------------- UGE Accounting Response -------------------------
        # First check the Accounting Response Queue which gets filled by a
        #
        # separate process and may contain the corresponding JobInfo
        job_info_lst = []
        # Get one snapshot of current Accounting Response Queue
        while self.__ugeService.ugeAcctJobInfoRes_Q.qsize():
            job_info_lst.append(self.__ugeService.ugeAcctJobInfoRes_Q.get())

        # if the jobInfo found in the list then return the jobInfo
        # and put all the Queue items back into __ugeAcctJobInfoRes_Q
        jobInfo = None
        while job_info_lst:
            jobInfo_tmp: UGEJobInfo = job_info_lst.pop(0)
            if jobInfo_tmp.cluster == cluster and jobInfo_tmp.jobid == jobId \
                    and jobInfo_tmp.taskid == taskid:
                jobInfo = jobInfo_tmp
            else:
                self.__ugeService.ugeAcctJobInfoRes_Q.put(jobInfo_tmp)
        # return the jobInfo if it was found
        if jobInfo:
            # The job has been finished and no more data should be aggregated for this job
            finishedJob = cluster + '_' + sched + '_' + str(jobId) + ("." + str(taskid) if taskid else "")
            finJobDB = FinishedJobs()
            finJobDB.store(finishedJob)
            return jobInfo

        # -------------- UGE Restful API -------------------------
        # Otherwise, set a UGERest query and get the running JobInfo if exists
        #
        uge_ip = self.config.getUGE_Addr()[cluster_inx]
        uge_port = self.config.getUGE_Port()[cluster_inx]

        # Call UGERestful API to collect Job Info
        jobInfo = self.__ugeService.getUGERestJobInfo(uge_ip, uge_port, jobId, taskid)

        # return if jobInfo could be collected successful from UGERest
        if jobInfo:
            # Complete the Object
            jobInfo.cluster = cluster
            jobInfo.sched_type = 'uge'
            return jobInfo

        # -------------- UGE Accounting Request -------------------------
        else:
            # If jobInfo was not found in UGE Accounting Response Queue
            # and could not be retrieved by UGERest call, then:
            #  - It might have been already finished but have not been caught by UGEAccounting RPC
            #  - Due to a delay in UGERest it has be requested next round
            # therefore, we put a request for next UGE Accounting RPC
            ugeAcctReq = cluster + '_' + str(jobId) + ("." + str(taskid) if taskid else ".0")
            self.__ugeService.ugeAcctJobIdReq_Q.put(ugeAcctReq)
            # Then, we create an empty JobInfo object with UNDEF status and return it
            jobInfo = UGEJobInfo()
            jobInfo.cluster = cluster
            jobInfo.sched_type = sched
            jobInfo.jobid = jobId
            jobInfo.taskid = (taskid if taskid else None)
            jobInfo.status = JobInfo.Status.UNDEF
            return jobInfo


#
# The Super class for all type of JobInfo objects which contains the necessary attributes
#
class JobInfo(object):
    def __init__(self):
        self.jobid = None
        self.cluster = None
        self.sched_type = None
        self.taskid = None
        self.status = self.Status.NONE
        self.jobName = None
        self.queue = None
        self.exec_host = None
        self.num_cpu = None
        self.submit_time = None
        self.start_time = None
        self.end_time = None
        self.username = None

    # This function returns a unique ID for every objects with the same JobID, Scheduler, and cluster
    def uniqID(self):
        # calculate the MD5 hash
        obj_id = ''.join(filter(lambda x: x not in [None, 0, '0'],
                                [self.sched_type, self.cluster, self.jobid, self.taskid]))
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
        UNDEF = 7



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
        self.failed_no = None
        self.q_del = []

#
# In any case of Error, Exception, or Mistake JobSchedulerException will be raised
#
class JobSchedulerException(Exception):
    def __init__(self, message):
        super(JobSchedulerException, self).__init__(message)
        self.message = "\n [Error] _JOB_SCHED_: " + message + "\n"

    def getMessage(self):
        return self.message


if __name__ == "__main__":
    import sys, time, os
    from tabulate import tabulate
    scheduler = JobScheduler()
    jobid = int(sys.argv[1])
    taskId = None
    if len(sys.argv) > 2:
        taskId = int(sys.argv[2])
    while True:
        jobinfo = scheduler.getJobInfo('genius', 'uge', jobid, taskId)
        # ---> Print jobinfo
        jobTbl = []
        for attr in [atr for atr in dir(jobinfo) if (not atr.startswith('__'))
                                                   and (not callable(getattr(jobinfo, atr)))]:
            jobTbl.append([attr, getattr(jobinfo, attr)])

        os.system("clear")
        print(tabulate(jobTbl, headers=["Attribute:", "Value:"], tablefmt="github") + "\n")

        # if jobinfo.status is JobInfo.Status.FINISHED:
        #     break
        time.sleep(5)

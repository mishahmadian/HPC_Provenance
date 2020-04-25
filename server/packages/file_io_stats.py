# -*- coding: utf-8 -*-
"""
    The main module which contains essential classes for collecting I/O statistics from file system.
    this package only supports LUSTRE fle system at this moment and supports following functions:

        1. Receiving IO stats from Luster servers which are collected by Provenance_agent services.
        2. Organizing collected data by mapping them to their corresponding object and then placing
            them into the related Queues for later process (i.e. Data Aggregation)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from communication import ServerConnection, CommunicationExp
from config import ServerConfig, ConfigReadExcetion
from multiprocessing import Process, Queue, Event
from exceptions import ProvenanceExitExp
from persistant import FinishedJobs
from typing import Dict, List, Set
from logger import log, Mode
import hashlib
import ctypes
import json

#
# This Class defines a new process which listens to the incoming port and collects
# I/O statistics that are sent from File system (Lustre) agents and put them
# into two queues (MSDStat_Q & OSSStat_Q)
#
class IOStatsListener(Process):
    def __init__(self, MSDStat_Q: Queue, OSSStat_Q: Queue, stop_flag: Event):
        Process.__init__(self)
        self.MSDStat_Q = MSDStat_Q
        self.OSSStat_Q = OSSStat_Q
        self.finishedJobs = FinishedJobs()
        self.config = ServerConfig()
        self._shut_down = stop_flag
        try:
            self.__MDS_hosts = self.config.getMDS_hosts()
            self.__OSS_hosts = self.config.getOSS_hosts()

        except ConfigReadExcetion as confExp:
            log(Mode.FILE_IO_STATS, confExp.getMessage())

    # Implement Process.run()
    def run(self):
        try:
            # Create server connection
            comm = ServerConnection()
            # Start collecting IO statistics
            # ioStats_recv function will take care of incoming data
            comm.Collect_io_stats(self.ioStats_receiver)

        except CommunicationExp as commExp:
            log(Mode.FILE_IO_STATS, commExp.getMessage())

        except ProvenanceExitExp:
            pass

        except Exception as exp:
            log(Mode.FILE_IO_STATS, str(exp))

    # This function will be triggered as soon as RabbitMQ receives data from
    # agents on jobStat queue
    def ioStats_receiver(self, ch, method, properties, body):
        # ---------- Terminate if the stop_flag was set ----------------
        if self._shut_down.is_set():
            ch.stop_consuming()
            return

        mdsStatObjLst : List[MDSDataObj] = []
        ossStatObjLst : List[OSSDataObj] = []
        # Get the list of those jobs which are already finished
        finished_jobIds = self.finishedJobs.getAll()
        # convert the message to JSON
        io_stat_map = json.loads(body.decode("utf-8"))
        # The server host name
        lustre_server = io_stat_map["server"]
        # Check whether the IO stat data comes from MDS or OSS.
        # Then choose the proper function
        if lustre_server in self.__MDS_hosts:
            # Then data should be processed for MDS
            blockedJobs, mdsStatObjLst = self.__parseIoStats_mds(io_stat_map, finished_jobIds)
            # Manage Finished Job IDs
            self.__refine_finishedJobs(lustre_server, blockedJobs, finished_jobIds)

        elif lustre_server in self.__OSS_hosts:
            # Parse the OSS IO stats
            blockedJobs, ossStatObjLst = self.__parseIoStats_oss(io_stat_map, finished_jobIds)
            # Manage Finished Job IDs
            self.__refine_finishedJobs(lustre_server, blockedJobs, finished_jobIds)

        else:
            # Otherwise the data should be processed for OSS
            raise IOStatsException("The Source of incoming data does not match "
                                    +" with MDS/OSS hosts in 'server.conf'")

        # Put mdsStatObjLst into the MSDStat_Q
        if mdsStatObjLst:
            self.MSDStat_Q.put(mdsStatObjLst)
        # Put ossStatObjs into OSSStat_Q
        if ossStatObjLst:
            self.OSSStat_Q.put(ossStatObjLst)


    # this method finds the finished_Job_IDs that no more exist on Lustre JobStat
    def __refine_finishedJobs(self, server: str, blockedJobs: Set[str], finished_jobIds: Dict):
        # List of invalid JobIds
        invalid_fin_ids = []

        # Get the Finished Jobs List of this server
        finJobList = finished_jobIds[server]
        # Check and see if the JobId is already cleared by the server
        # If so, then put it in the invalid list
        for jobIds in finJobList:
            if jobIds not in blockedJobs:
                invalid_fin_ids.append(jobIds)

        # Now refine the list of Finished JobIds by removing the unnecessary Ids
        if invalid_fin_ids:
            self.finishedJobs.refine(server, invalid_fin_ids)

    #
    # Convert/Map received data from MDS servers into a list of "MDSDataObj" data type
    #@staticmethod
    def __parseIoStats_mds(self, data: Dict[str, str], finished_jobIds: dict) -> (Set, List):
        # Create a List of MDSDataObj
        mdsObjLst: List[MDSDataObj] = []
        # Create a list for those jobs which will be blocked since they're already finished
        blockedJobs: Set[str] = set()
        timestamp = data["timestamp"]
        serverHost = data["server"]
        serverTarget = data["fstarget"]
        max_age = int(data["maxAge"])
        # Filter out a group of received JobStats of different jobs
        jobstatLst = data["output"].split("job_stats:")
        # drop the first element because its always useless
        del jobstatLst[0]
        # Split jobs in JobStat list by '-' and skip the first element which is empty
        jobstatLst = jobstatLst[0].split('-')[1:]
        # Iterate over the jobstatLst
        for jobstat in jobstatLst:
            # define mdsObj but do not initiate it
            mdsObj = None
            # Parse the JobStat output line by line
            for line in jobstat.splitlines():
                # skip empty lines
                if not line.strip():
                    continue
                # First column of each line is the attribute
                attr = line.split(':')[0].strip()
                # extract the job_id value which
                if "job_id" in attr:
                    # get the id
                    jobid = line.split(':')[1].strip()

                    # If jobid[.taskid] appears among the list of finished jobs,
                    # then ignore the Jobstats data of this job
                    if jobid in finished_jobIds[serverHost]:
                        blockedJobs.add(jobid)
                        break
                    # Create new MDSDataObj
                    mdsObj = MDSDataObj()
                    # if the id format is not compatible with "cluster_scheduler_ID" then it's a process id
                    if '_' not in jobid:
                        mdsObj.procid = jobid
                    # Otherwise, it is a JOB
                    else:
                        mdsObj.cluster, mdsObj.sched_type, mdsObj.jobid = jobid.split('_')\

                        # if the jobid is separated by '.' then it means the job is an array job
                        if '.' in mdsObj.jobid:
                            mdsObj.jobid, mdsObj.taskid = mdsObj.jobid.split('.')

                # Snapshot from Lustre reports
                elif "snapshot_time" in attr:
                    snapshot = line.split(':')[1].strip()
                    mdsObj.snapshot_time = int(snapshot) if snapshot.isnumeric() else snapshot
                # Pars the attributes that are available in MDSDataObj
                else:
                    # skip the unwanted attributes
                    if attr not in mdsObj.__dict__.keys():
                        continue
                    # Set the corresponding attribute in the MDSDataObj object
                    value = line.split(':')[2].split(',')[0].strip()
                    if value.isnumeric(): value = int(value)
                    setattr(mdsObj, attr, value)

            if mdsObj:
                # Timestamp recorded on agent side
                mdsObj.timestamp = timestamp
                # Maximum Age of the JobStat Rec on Server
                mdsObj.maxAge = max_age
                # The host name of the server
                mdsObj.mds_host = serverHost
                # The MDT target
                mdsObj.mdt_target = serverTarget
                # Put the mdsObj into a list
                mdsObjLst.append(mdsObj)

        # Return the JobStat output in form of MDSDataObj data type
        return blockedJobs, mdsObjLst

    #
    # Convert/Map received data from MDS servers into "OSSDataObj" data type
    #@staticmethod
    def __parseIoStats_oss(self, data: Dict[str, str], finished_jobIds: dict) -> (Set, List):
        # Create a List of OSSDataObj
        ossObjLst: List[OSSDataObj] = []
        # Create a list for those jobs which will be blocked since they're already finished
        blockedJobs: Set[str] = set()
        timestamp = data["timestamp"]
        serverHost = data["server"]
        serverTarget = data["fstarget"]
        max_age = int(data["maxAge"])
        # Filter out a group of received JobStats of different jobs
        jobstatLst = data["output"].split("job_stats:")
        # drop the first element because its always useless
        del jobstatLst[0]
        # Split jobs in JobStat list by '-' and skip the first element which is empty
        jobstatLst = jobstatLst[0].split('-')[1:]
        # Iterate over the jobStatLst
        for jobstat in jobstatLst:
            # define ossObj but do not initiate it
            ossObj = None
            # Parse the JobStat output line by line
            for line in jobstat.splitlines():
                # skip empty lines
                if not line.strip():
                    continue
                # First column of each line is the attribute
                attr = line.split(':')[0].strip()
                # extract the job_id value which is like: $CLUSTER_SCHED_$JOBID
                if "job_id" in attr:
                    # get the id
                    jobid = line.split(':')[1].strip()

                    # If jobid[.taskid] appears among the list of finished jobs,
                    # then ignore the Jobstats data of this job
                    if jobid in finished_jobIds[serverHost]:
                        blockedJobs.add(jobid)
                        break
                    # Create new OSSDataObj
                    ossObj = OSSDataObj()
                    # if the id format is not compatible with "cluster_scheduler_ID" then it's a process id
                    if '_' not in jobid:
                        ossObj.procid = jobid
                    # Otherwise, it is a JOB
                    else:
                        ossObj.cluster, ossObj.sched_type, ossObj.jobid = jobid.split('_')\

                        # if the jobid is separated by '.' then it means the job is an array job
                        if '.' in ossObj.jobid:
                            ossObj.jobid, ossObj.taskid = ossObj.jobid.split('.')

                # Snapshot from Lustre reports
                elif "snapshot_time" in attr:
                    snapshot = line.split(':')[1].strip()
                    ossObj.snapshot_time = int(snapshot) if snapshot.isnumeric() else snapshot
                # Parse read_bytes and write_bytes in a different way
                elif "_bytes" in attr:
                    # a set of related parameters for Read and Write operations
                    # attr_ext holds the position of MIN/MAX/SUM values in the  for each Read/Write
                    # operation in the JobStats output from OSS (The first item is read/write itself)
                    attr_ext = {"" : 2, "_min" :4 , "_max" : 5, "_sum" : 6}
                    for ext in attr_ext:
                        inx = attr_ext[ext]
                        objattr = attr + ext # define the name of the attr in OSSDataObj
                        delim2 = ',' if  inx != 6 else '}' # Splitting the JobStat output is weird!
                        # Set the corresponding attribute in the OSSDataObj object
                        value = line.split(':')[inx].split(delim2)[0].strip()
                        if value.isnumeric(): value = int(value)
                        setattr(ossObj, objattr, value)
                # Pars the attributes that are available in OSSDataObj
                else:
                    # skip the unwanted attributes
                    if attr not in ossObj.__dict__.keys():
                        continue
                    # Set the corresponding attribute in the OSSDataObj object
                    value = line.split(':')[2].split(',')[0].strip()
                    if value.isnumeric(): value = int(value)
                    setattr(ossObj, attr, value)

            if ossObj:
                # Timestamp recorded on agent side
                ossObj.timestamp = timestamp
                # Maximum Age of the JobStat Rec on Server
                ossObj.maxAge = max_age
                # The host name of the server
                ossObj.oss_host = serverHost
                # The OST target
                ossObj.ost_target = serverTarget
                # Put the ossObj into a list
                ossObjLst.append(ossObj)

        # Rerun the JobStat output in form of OSSDataObj data type
        return blockedJobs, ossObjLst

#
# Object class that holds the process data by "ioStats_mds_decode".
# this class will be imported in other packages/classes
#
class MDSDataObj(object):
    def __init__(self):
        self.mds_host = None
        self.mdt_target = None
        self.maxAge = 0
        self.timestamp = 0
        self.jobid = None
        self.taskid = None
        self.cluster = None
        self.sched_type = None
        self.procid = None
        self.snapshot_time = 0
        self.open = 0
        self.close = 0
        self.mknod = 0
        self.link = 0
        self.unlink = 0
        self.mkdir = 0
        self.rmdir = 0
        self.rename = 0
        self.getattr = 0
        self.setattr = 0
        self.samedir_rename = 0
        self.crossdir_rename = 0

    # Overriding the Hash function for this object
    def __hash__(self):
        # calculate the hash
        hashVal = hash((self.jobid, self.cluster, self.sched_type))
        # make sure the value is always positive (we don't want negative hash to be used as ID)
        return ctypes.c_size_t(hashVal).value

    # This function returns a unique ID for every objects with the same JobID, Scheduler, and cluster
    def uniqID(self):
        if self.procid:
            # No hash for this object if jobID is not defined
            return None
        # calculate the MDS hash
        obj_id = ''.join(filter(None, [self.sched_type, self.cluster, self.jobid, self.taskid]))
        hash_id = hashlib.md5(obj_id.encode(encoding='UTF=8'))
        return hash_id.hexdigest()

    # Return a dictionary format of all attrs and their values
    def to_dict(self) -> dict:
        attrDict = {}
        # collect all available attributes
        attrs = [atr for atr in dir(self) if (not atr.startswith('__')) and (not callable(getattr(self, atr)))]
        for attr in attrs:
            attrDict[attr] = getattr(self, attr)
        # append the unique ID
        attrDict['uid'] = self.uniqID()
        #
        return  attrDict


#
# Object class that holds the process data by "ioStats_OSS_decode".
# this class will be imported in other packages/classes
#
class OSSDataObj(object):
    def __init__(self):
        self.oss_host = None
        self.ost_target = None
        self.maxAge = 0
        self.timestamp = 0
        self.jobid = None
        self.taskid =None
        self.cluster = None
        self.sched_type = None
        self.procid = None
        self.snapshot_time = 0
        self.read_bytes = 0
        self.read_bytes_min = 0
        self.read_bytes_max = 0
        self.read_bytes_sum = 0
        self.write_bytes = 0
        self.write_bytes_min = 0
        self.write_bytes_max = 0
        self.write_bytes_sum = 0
        self.getattr = 0
        self.setattr = 0
        self.punch = 0
        self.sync = 0
        self.destroy = 0
        self.create = 0

    # Overriding the Hash function for this object
    def __hash__(self):
        # calculate the hash
        hashVal = hash((self.jobid, self.cluster, self.sched_type))
        # make sure the value is always positive (we don't want negative hash to be used as ID)
        return ctypes.c_size_t(hashVal).value

    # This function returns a unique ID for every objects with the same JobID, Scheduler, and cluster
    # More reliable over hash function!!
    def uniqID(self):
        if self.procid:
            # No hash for this object if jobID is not defined
            return None
        # calculate the OSS hash
        obj_id = ''.join(filter(None, [self.sched_type, self.cluster, self.jobid, self.taskid]))
        hash_id = hashlib.md5(obj_id.encode(encoding='UTF=8'))
        return hash_id.hexdigest()

    # Return a dictionary format of all attrs and their values
    def to_dict(self) -> dict:
        attrDict = {}
        # collect all available attributes
        attrs = [atr for atr in dir(self) if (not atr.startswith('__')) and (not callable(getattr(self, atr)))]
        for attr in attrs:
            attrDict[attr] = getattr(self, attr)
        # append the unique ID
        attrDict['uid'] = self.uniqID()
        #
        return attrDict

#
# In case of error the following exception can be raised
#
class IOStatsException(Exception):
    def __init__(self, message):
        super(IOStatsException, self).__init__(message)
        self.message = "\n [Error] _IO-STATS_: " + message + "\n"

    def getMessage(self):
        return self.message

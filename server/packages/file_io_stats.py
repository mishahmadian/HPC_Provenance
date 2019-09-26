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
from multiprocessing import Process, Queue
from typing import Dict, List
import ctypes
import json

#
# This Class defines a new process which listens to the incoming port and collects
# I/O statistics that are sent from File system (Lustre) agents and put them
# into two queues (MSDStat_Q & OSSStat_Q)
#
class IOStatsListener(Process):
    def __init__(self, MSDStat_Q: Queue, OSSStat_Q: Queue):
        Process.__init__(self)
        self.MSDStat_Q = MSDStat_Q
        self.OSSStat_Q = OSSStat_Q
        self.config = ServerConfig()
        try:
            self.__MDS_hosts = self.config.getMDS_hosts()
            self.__OSS_hosts = self.config.getOSS_hosts()

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

        # print(self._parent_pid)

    # Implement Process.run()
    def run(self):
        try:
            # Create server connection
            comm = ServerConnection()
            # Start collecting IO statistics
            # ioStats_recv function will take care of incoming data
            comm.Collect_io_stats(self.ioStats_receiver)

        except CommunicationExp as commExp:
            print(commExp.getMessage())

        except Exception as exp:
            print(str(exp))

    # This function will be triggered as soon as RabbitMQ receives data from
    # agents on jobStat queue
    def ioStats_receiver(self, ch, method, properties, body):
        io_stat_map = json.loads(body.decode("utf-8"))
        # Check whether the IO stat data comes from MDS or OSS.
        # Then choose the proper function
        if io_stat_map["server"] in self.__MDS_hosts:
            #print("I'm here 1_MDS")
            # Then data should be processed for MDS
            mdsStatObjLst = self.__parseIoStats_mds(io_stat_map)
            #print("I'm here 2_MDS")
            # Put mdsStatObjLst into the MSDStat_Q
            #==for mdsStatObj in mdsStatObjLst:
            self.MSDStat_Q.put(mdsStatObjLst)

        elif io_stat_map["server"] in self.__OSS_hosts:
            # Parse the OSS IO stats
            #print("I'm here 1_OSS")
            ossStatObjLst = self.__parseIoStats_oss(io_stat_map)
            #print("I'm here 2_OSS")
            # Put ossStatObjs into OSSStat_Q
            #==for ossStatObj in ossStatObjLst:
            self.OSSStat_Q.put(ossStatObjLst)
        else:
            # Otherwise the data should be processed for OSS
            raise IOStatsException("The Source of incoming data does not match "
                                    +" with MDS/OSS hosts in 'server.conf'")

    #
    # Convert/Map received data from MDS servers into a list of "MDSDataObj" data type
    #@staticmethod
    def __parseIoStats_mds(self, data: Dict[str, str]) -> List:
        # Create a List of MDSDataObj
        mdsObjLst: List[MDSDataObj] = []
        timestamp = data["timestamp"]
        serverHost = data["server"]
        # Filter out a group of received JobStats of different jobs
        jobstatLst = data["output"].split("job_stats:")
        # drop the first element because its always empty
        del jobstatLst[0]
        # Iterate over the jobstatLst
        for jobstat in jobstatLst:
            # Create new MDSDataObj
            mdsObj = MDSDataObj()
            # Timestamp recorded on agent side
            mdsObj.timestamp = timestamp
            # The host name of the server
            mdsObj.mds_host = serverHost
            # Parse the JobStat output line by line
            for line in data["output"].splitlines():
                # skip empty lines
                if not line.strip():
                    continue
                # First column of each line is the attribute
                attr = line.split(':')[0].strip()
                # extract the job_id value which is like: $CLUSTER_SCHED_$JOBID
                if "job_id" in attr:
                    mdsObj.cluster, mdsObj.sched_type, mdsObj.jobid = \
                                       line.split(':')[1].strip().split('_')
                # Snapshot from Lustre reports
                elif "snapshot_time" in attr:
                    mdsObj.snapshot_time = line.split(':')[1].strip()
                # Pars the attributes that are available in MDSDataObj
                else:
                    # skip the unwanted attributes
                    if attr not in mdsObj.__dict__.keys():
                        continue
                    # Set the corresponding attribute in the MDSDataObj object
                    value = line.split(':')[2].split(',')[0].strip()
                    setattr(mdsObj, attr, value)

            # Put the mdsObj into a list
            mdsObjLst.append(mdsObj)
        # Return the JobStat output in form of MDSDataObj data type
        return mdsObjLst

    #
    # Convert/Map received data from MDS servers into "OSSDataObj" data type
    #@staticmethod
    def __parseIoStats_oss(self, data: Dict[str, str]) -> List:
        # Create a List of OSSDataObj
        ossObjLst: List[OSSDataObj] = []
        timestamp = data["timestamp"]
        serverHost = data["server"]
        # Filter out a group of received JobStats of different jobs
        jobstatLst = data["output"].split("job_stats:")
        # drop the first element because its always empty
        del jobstatLst[0]
        # Iterate over the jobStatLst
        for jobstat in jobstatLst:
            # Create new OSSDataObj
            ossObj = OSSDataObj()
            # Timestamp recorded on agent side
            ossObj.timestamp = timestamp
            # The host name of the server
            ossObj.oss_host = serverHost
            # Parse the JobStat output line by line
            for line in data["output"].splitlines():
                # skip empty lines
                if not line.strip():
                    continue
                # First column of each line is the attribute
                attr = line.split(':')[0].strip()
                # extract the job_id value which is like: $CLUSTER_SCHED_$JOBID
                if "job_id" in attr:
                    ossObj.cluster, ossObj.sched_type, ossObj.jobid = \
                                       line.split(':')[1].strip().split('_')
                # Snapshot from Lustre reports
                elif "snapshot_time" in attr:
                    ossObj.snapshot_time = line.split(':')[1].strip()
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
                        setattr(ossObj, objattr, value)
                # Pars the attributes that are available in OSSDataObj
                else:
                    # skip the unwanted attributes
                    if attr not in ossObj.__dict__.keys():
                        continue
                    # Set the corresponding attribute in the OSSDataObj object
                    value = line.split(':')[2].split(',')[0].strip()
                    setattr(ossObj, attr, value)

            # Put the ossObj into a list
            ossObjLst.append(ossObj)
        # Rerun the JobStat output in form of OSSDataObj data type
        return ossObjLst

#
# Object class that holds the process data by "ioStats_mds_decode".
# this class will be imported in other packages/classes
#
class MDSDataObj(object):
    def __init__(self):
        self.mds_host = None
        self.timestamp = 0
        self.jobid = None
        self.cluster = None
        self.sched_type = None
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


#
# Object class that holds the process data by "ioStats_OSS_decode".
# this class will be imported in other packages/classes
#
class OSSDataObj(object):
    def __init__(self):
        self.oss_host = None
        self.timestamp = 0
        self.jobid = None
        self.cluster = None
        self.sched_type = None
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
        self.destroy = 0
        self.create = 0

    # Overriding the Hash function for this object
    def __hash__(self):
        # calculate the hash
        hashVal = hash((self.jobid, self.cluster, self.sched_type))
        # make sure the value is always positive (we don't want negative hash to be used as ID)
        return ctypes.c_size_t(hashVal).value

#
# In case of error the following exception can be raised
#
class IOStatsException(Exception):
    def __init__(self, message):
        super(IOStatsException, self).__init__(message)
        self.message = "\n [Error] _IO-STATS_: " + message + "\n"

    def getMessage(self):
        return self.message

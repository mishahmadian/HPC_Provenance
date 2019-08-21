# -*- coding: utf-8 -*-
"""
    The main package contains essential classes for collecting I/O statistics from file system.
    this package only supports LUSTRE fle system at this moment and supports following functions:

        1. Receiving IO stats from Luster servers which are collected by Provenance_agent services.
        2. Collecting Changelog data from a Lustre client
            - We assume the Provenance server has already mounted a lustre client
        3. Organizing collected data and placing them in related Queues for later process (Data Aggregation)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from communication import ServerConnection, CommunicationExp
from config import ServerConfig, ConfigReadExcetion
from multiprocessing import Process
from queue import Queue
import signal
import json
import os
#
# This Class defines a new process which listens to the incomming port and collects
# I/O statistics that are sent from File system (Lustre) agents and put them
# into a queue (fsIOstat_Q)
#
class IOStatsListener(Process):
    def __init__(self, fsIOstat_Q):
        Process.__init__(self)
        self.fsIOstat_Q = fsIOstat_Q
        self.config = ServerConfig()
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
        io_stat_map = json.loads(body.decode())
        # Check whether the IO stat data comes from MDS or OSS.
        # Then choose the proper function
        if io_stat_map["server"] in self.config.getMDS_hosts():
            # Then data should be processed for MDS
            mdsStatObjLst = self.__ioStats_mds_decode(io_stat_map)
            # Put mdsStatObjLst items into the fsIOstat_Q
            for mdsStatObj in mdsStatObjLst:
                self.fsIOstat_Q.put(mdsStatObj)

        elif io_stat_map["server"] in self.config.getOSS_hosts():
            # Parse the OSS IO stats
            ossStatObjLst = self.__ioStats_oss_decode(io_stat_map)
            # Put ossStatObjs into fsIOstat_Q
            for ossStatObj in ossStatObjLst:
                self.fsIOstat_Q.put(ossStatObj)
        else:
            # Otherwise the data should be processed for OSS
            raise IOStatsExcetion("The destination of incoming data does not match "
                                    +" with MDS/OSS hosts in 'server.conf'")

    #
    # Convert/Map received data from MDS servers into a list of "MDSDataObj" data type
    @staticmethod
    def __ioStats_mds_decode(data):
        # Create a List of MDSDataObj
        mdsObjLst = []
        timestamp = data["timestamp"]
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
            # Parse the Jobstat output line by line
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
        # Retrun the jobstat output in form of MDSDataObj data type
        return mdsObjLst

    #
    # Convert/Map received data from MDS servers into "OSSDataObj" data type
    @staticmethod
    def __ioStats_oss_decode(data):
        # Create a List of OSSDataObj
        ossObjLst = []
        timestamp = data["timestamp"]
        # Filter out a group of received JobStats of different jobs
        jobstatLst = data["output"].split("job_stats:")
        # drop the first element because its always empty
        del jobstatLst[0]
        # Iterate over the jobstatLst
        for jobstat in jobstatLst:
            # Create new OSSDataObj
            ossObj = OSSDataObj()
            # Timestamp recorded on agent side
            ossObj.timestamp = timestamp
            # Parse the Jobstat output line by line
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
                    attr_ext = {"" : 2, "_min" :4 , "_max" : 5, "_sum" : 6}
                    for ext in attr_ext:
                        inx = attr_ext[ext]
                        objattr = attr + ext # define the name of the attr in OSSDataObj
                        delim2 = ',' if  inx != 6 else '}' # Splitting the jobstat output is weird!
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
        # Retrun the jobstat output in form of OSSDataObj data type
        return ossObjLst

#
# Object class that holds the process data by "ioStats_mds_decode".
# this class will be imported in other packages/classes
#
class MDSDataObj:
    def __init__(self):
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

#
# Object class that holds the process data by "ioStats_OSS_decode".
# this class will be imported in other packages/classes
#
class OSSDataObj:
    def __init__(self):
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
        self.create

#
# This Class defines a new process to collect Lustre changelog data from
# Lustre client. The changelog has to be registered on MGS server.
#
class ChangeLogCollector(Process):
    def __init__(self, chLog_Q):
        Process.__init__(self)
        self.chLog_Q = chLog_Q
        self.config = ServerConfig()

    # Implement Process.run()
    def run(self):
        pass
#
# In case of error the follwoing exception can be raised
#
class IOStatsExcetion(Exception):
    def __init__(self, message):
        super(IOStatsExcetion, self).__init__(message)
        self.message = "\n [Error] _IO-STATS_: " + message + "\n"

    def getMessage(self):
        return self.message

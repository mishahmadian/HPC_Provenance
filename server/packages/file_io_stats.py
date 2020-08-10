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
from yaml import Loader, load as yaml_load
from exceptions import ProvenanceExitExp
from persistant import FinishedJobs
from threading import Thread, Lock, get_ident
from typing import Dict, List, Set
from functools import partial
from logger import log, Mode
import hashlib
import ctypes
import json
import time

# Try to load the PyYaml if it's been compiled against LibYAML
# which is more efficient (Looks like CLoader has a bug!)
# try:
#     from yaml import CLoader as Yaml_Loader
# except ImportError:
#     from yaml import Loader as Yaml_Loader

#
# This Class defines a new process which listens to the incoming port and collects
# I/O statistics that are sent from File system (Lustre) agents and put them
# into two queues (MSDStat_Q & OSSStat_Q)
#
class IOStatsListener(Process):
    def __init__(self, MSDStat_Q: Queue, OSSStat_Q: Queue, serverStats_Q: Queue, stop_flag: Event):
        Process.__init__(self)
        self.serverStats_Q = serverStats_Q
        self.MSDStat_Q = MSDStat_Q
        self.OSSStat_Q = OSSStat_Q
        self.finishedJobs = FinishedJobs()
        self.config = ServerConfig()
        self._shut_down = stop_flag
        self._threads: Dict[int, Thread] = {}
        self._lock = Lock()
        try:
            # Get IO Stat Listener Params
            self._iostats_queue = self.config.getIOListener_Queue()
            self._iostats_exchange = self.config.getIOListener_Exch()
            self.__MDS_hosts = self.config.getMDS_hosts()
            self.__OSS_hosts = self.config.getOSS_hosts()
            self._jobIdVars = self.config.getJobIdVars()

        except ConfigReadExcetion as confExp:
            log(Mode.FILE_IO_STATS, confExp.getMessage())

    # Implement Process.run()
    def run(self):
        try:
            # Create server connection
            comm = ServerConnection()
            # Start collecting IO statistics
            # ioStats_recv function will take care of incoming data
            comm.start_listener(self._iostats_queue, self._iostats_exchange, self.on_iostats_recieved)

        except CommunicationExp as commExp:
            log(Mode.FILE_IO_STATS, commExp.getMessage())

        except ProvenanceExitExp:
            pass

        except Exception as exp:
            log(Mode.FILE_IO_STATS, str(exp))

    # This method gets called when a new data fetched from the message broker queue
    # Then, it will handle the data process in multi-thread manner
    def on_iostats_recieved(self, channel, method, properties, body):
        # Get the unique delivery_tag of this AMQP message:
        delivery_tag = method.delivery_tag
        # Get current connection
        connection = channel._connection

        # Terminate if the stop_flag was set
        if self._shut_down.is_set():
            self._terminate(connection, channel, delivery_tag)
            return

        # Create a Thread for processing incoming I/O Stats Data
        thrd = Thread(target=self.ioStats_processing, args=(connection, channel, delivery_tag, body, self._lock))
        thrd.start()
        # Keep the history of threads
        self._threads[thrd.ident] = thrd


    # This function will be triggered as soon as RabbitMQ receives data from
    # agents on jobStat queue
    def ioStats_processing(self, connection, channel, delivery_tag, body, lock):

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
            # Parse the MDS data in a separate thread
            blockedJobs, mdsStatObjLst = self.__parseIoStats_mds(io_stat_map, finished_jobIds)

        elif lustre_server in self.__OSS_hosts:
            # Parse the OSS data in a separate thread
            blockedJobs, ossStatObjLst = self.__parseIoStats_oss(io_stat_map, finished_jobIds)

        else:
            # Otherwise the data should be processed for OSS
            raise IOStatsException("The Source of incoming data does not match "
                                    +" with MDS/OSS hosts in 'server.conf'")

        # Manage Finished Job IDs
        with lock:
            self.__refine_finishedJobs(lustre_server, blockedJobs, finished_jobIds)

        # Put mdsStatObjLst into the MSDStat_Q
        if mdsStatObjLst:
            self.MSDStat_Q.put(mdsStatObjLst)
        # Put ossStatObjs into OSSStat_Q
        if ossStatObjLst:
            self.OSSStat_Q.put(ossStatObjLst)
        # Put Server Resource Status in the
        if io_stat_map.get('serverLoad', None) or io_stat_map.get('serverMemory', None):
            self.serverStats_Q.put(f"{io_stat_map['server']};"
                                   f"{io_stat_map['timestamp']};"
                                   f"{io_stat_map.get('serverLoad', [0.0, 0.0, 0.0])};"
                                   f"{io_stat_map.get('serverMemory', [0, 0])}")

        # Send ACK message after the data processing is done
        threadsafe_ACK = partial(ServerConnection.ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(threadsafe_ACK)

    # Terminate the File IO States loop
    def _terminate(self, connection, channel, delivery_tag):
        # Reject the incoming data and put it back into the queue
        channel.basic_reject(delivery_tag, requeue=True)
        # signal consuming loop to exit
        channel.stop_consuming()
        # Close the channel
        if channel.is_open:
            channel.close()
        # Close the connection
        if connection and connection.is_open:
            connection.close()
        # Wait for the processing threads to finish
        for thr in list(self._threads.values()):
            if thr and thr.is_alive():
                thr.join()


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
    #
    def __parseIoStats_mds(self, data: Dict[str, str], finished_jobIds: Dict) -> (Set, List):
        # Create a List of MDSDataObj
        mdsObjLst: List[MDSDataObj] = []
        # Create a list for those jobs which will be blocked since they're already finished
        blockedJobs: Set[str] = set()
        timestamp = data["timestamp"]
        serverHost = data["server"]
        serverTarget = data["fstarget"]
        # Parse the received data which is in YAML format
        parsed_data = yaml_load(data["output"], Loader=Loader)
        # Proceed if data was parsed successfully
        if parsed_data and "job_stats" in parsed_data.keys():
            # Get the list of all job stats
            jobstatLst = parsed_data['job_stats']
            # Iterate over all job stats in the list
            for jobstat in jobstatLst:
                # define mdsObj but do not initiate it
                mdsObj = MDSDataObj()
                # Timestamp recorded on agent side
                mdsObj.timestamp = timestamp
                # The host name of the server
                mdsObj.mds_host = serverHost
                # The MDT target
                mdsObj.mdt_target = serverTarget
                # Extract job_id
                jobid = str(jobstat['job_id'])

                # if the id format is not compatible with "cluster_scheduler_ID" then it's a process id
                if not any([jobid.startswith(jobid_var) for jobid_var in self._jobIdVars]):
                    mdsObj.procid = jobid
                # Otherwise, it is a JOB
                else:
                    # If cluster_sched_jobid[.taskid] appears among the list of finished jobs,
                    # then ignore the JobStats data of this job
                    if jobid in finished_jobIds[serverHost]:
                        blockedJobs.add(jobid)
                        break

                    mdsObj.cluster, mdsObj.sched_type, mdsObj.jobid = jobid.split('_')
                    # if the jobid is separated by '.' then it means the job is an array job
                    if '.' in mdsObj.jobid:
                        mdsObj.jobid, mdsObj.taskid = mdsObj.jobid.split('.')

                # collect rest of the properties
                for attr, value in jobstat.items():
                    # Ignore anything else
                    if attr not in mdsObj.__dict__.keys():
                        continue

                    if isinstance(value, dict):
                        setattr(mdsObj, attr, value.get('samples', None))
                    else:
                        setattr(mdsObj, attr, value)

                # Put the mdsObj into a list
                mdsObjLst.append(mdsObj)

        # Return the JobStat output in form of MDSDataObj data type
        return blockedJobs, mdsObjLst

    #
    # Convert/Map received data from MDS servers into "OSSDataObj" data type
    #
    def __parseIoStats_oss(self, data: Dict[str, str], finished_jobIds: Dict) -> (Set, List):
        # Create a List of OSSDataObj
        ossObjLst: List[OSSDataObj] = []
        # Create a list for those jobs which will be blocked since they're already finished
        blockedJobs: Set[str] = set()
        timestamp = data["timestamp"]
        serverHost = data["server"]
        serverTarget = data["fstarget"]
        # Parse the received data which is in YAML format
        parsed_data = yaml_load(data["output"], Loader=Loader)
        # Proceed if data was parsed successfully
        if parsed_data and "job_stats" in parsed_data.keys():
            # Get the list of all job stats
            jobstatLst = parsed_data['job_stats']
            # Iterate over all job stats in the list
            for jobstat in jobstatLst:
                # define ossObj but do not initiate it
                ossObj = OSSDataObj()
                # Timestamp recorded on agent side
                ossObj.timestamp = timestamp
                # The host name of the server
                ossObj.oss_host = serverHost
                # The MDT target
                ossObj.ost_target = serverTarget
                # Extract job_id
                jobid = str(jobstat['job_id'])
                # If cluster_sched_jobid[.taskid] appears among the list of finished jobs,
                # then ignore the Jobstats data of this job
                if jobid in finished_jobIds[serverHost]:
                    blockedJobs.add(jobid)
                    break

                # if the id format is not compatible with "cluster_scheduler_ID" then it's a process id
                if not any([jobid.startswith(jobid_var) for jobid_var in self._jobIdVars]):
                    ossObj.procid = jobid
                # Otherwise, it is a JOB
                else:
                    ossObj.cluster, ossObj.sched_type, ossObj.jobid = jobid.split('_')
                    # if the jobid is separated by '.' then it means the job is an array job
                    if '.' in ossObj.jobid:
                        ossObj.jobid, ossObj.taskid = ossObj.jobid.split('.')

                # collect rest of the properties
                for attr, value in jobstat.items():
                    # skip the unwanted attributes
                    if attr not in ossObj.__dict__.keys():
                        continue

                    if isinstance(value, dict):
                        # treat these different
                        for attr_ext in ['min', 'max', 'sum']:
                            if attr_ext in value.keys():
                                setattr(ossObj, '_'.join([attr, attr_ext]), value.get(attr_ext))

                        setattr(ossObj, attr, value.get('samples', None))

                    else:
                        setattr(ossObj, attr, value)

                # Put the mdsObj into a list
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
        self.statfs = 0
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
        # We create uniqID for MDS data even for non-JOB (process) activities
        # to store them in influxDB
        if self.procid:
            obj_id = self.procid
        else:
            obj_id = ''.join(filter(None, [self.sched_type, self.cluster, self.jobid, self.taskid]))
        # calculate the OSS hash
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
        # We create uniqID for OSS data even for non-JOB (process) activities
        # to store them in influxDB
        if self.procid:
            obj_id = self.procid
        else:
            obj_id = ''.join(filter(None, [self.sched_type, self.cluster, self.jobid, self.taskid]))
        # calculate the OSS hash
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

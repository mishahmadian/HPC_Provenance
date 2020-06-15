# -*- coding: utf-8 -*-
"""
    The "aggregator" module containes the main Aggregator class which receives the collected data from agents
    and aggregate them all into a comprehensive and meaningful data to be stored in database or used as a query
    data for the Provenance API

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from scheduler import JobScheduler, JobInfo, UGEJobInfo, JobSchedulerException
from multiprocessing.managers import BaseManager, NamespaceProxy, DictProxy
from multiprocessing import Process, Event, Manager, Lock, Queue
from config import ServerConfig, ConfigReadExcetion
from threading import Event as Event_Thr, Thread
from file_io_stats import MDSDataObj, OSSDataObj
from db_operations import MongoOPs, InfluxOPs
from exceptions import ProvenanceExitExp
from collections import defaultdict
from persistant import FinishedJobs
from typing import List, Dict, Set
from file_op_logs import FileOpObj
from bisect import bisect_left
from logger import log, Mode
from uge_service import UGE
import subprocess
import time
import re

#------ Global Variable ------
# Timer Value
timer_val = 0.0

class Aggregator(Process):
    """
    Aggregate all MDS, OSS and File Operation data collected from different targets
    and store them into various databases.
    """
    def __init__(self, MSDStat_Q, OSSStat_Q, fileOP_Q, serverStats_Q, shutdown: Event):
        Process.__init__(self)
        self.serverStats_Q = serverStats_Q
        self.MSDStat_Q = MSDStat_Q
        self.OSSStat_Q = OSSStat_Q
        self.fileOP_Q = fileOP_Q
        self.jobInfo_Q = Queue()
        self.event_flag = Event()
        self.shutdown = shutdown
        self.timesUp = Event()
        self.currentTime = 0
        self.config = ServerConfig()
        try:
            self._interval = self.config.getAggrIntv()
            #self._timerIntv = self.config.getAggrTimer()
            self._mdtTargets = self.config.getMdtTargets()
            self._chLogUsers = self.config.getChLogsUsers()
            self._uge_clusters = self.config.getUGE_clusters()

        except ConfigReadExcetion as confExp:
            log(Mode.AGGREGATOR, confExp.getMessage())
            self.event_flag.set()


    # Implement Process.run()
    def run(self):
        # Register the ProvenanceObj to the Aggregator Manager along with the Proxy class
        self._AggregatorManager.register('ProvenanceObj', ProvenanceObj, self._ProvenanceObjProxy)
        # Start the Base manager
        provenanceObjManager = self._AggregatorManager()
        provenanceObjManager.start()

        try:
            # ugeService executes when [uge] section appears in server.conf
            ugeService = None
            # If UGE scheduler is defined then use it for scheduler
            if self._uge_clusters:
                ugeService = UGE(self.shutdown)
            # Create an instance of the scheduler
            jobScheduler = JobScheduler(UGE=ugeService)
            # Create a Timer Thread to be running for this process and change the
            # "timer_val" value every
            #==timer_flag = Event_Thr()
            #==timer = Thread(target=self._timer, args=(timer_flag, self._timerIntv,))
            #==timer.setDaemon(True)
            #==timer.start()

            # Create a shard Dictionary object which allows three process to update their values
            # This object will be created once only when aggregator process starts running
            provenanceTbl = Manager().dict()

            while not any([self.event_flag.is_set(), self.shutdown.is_set()]):
                # Record the current timestamp
                self.currentTime = time.time()
                # a list of Processes
                procList: List[Process] = []
                # Manage critical sections
                aggregatorLock = Lock()
                # reset the Times Up signal for all process
                self.timesUp.clear()

                #---------------------- AGGREGATE DATA -------------------------------
                # Aggregate MDS IO Stats into the Provenance Table
                procList.append(Process(target=self._aggregateFIO, args=(provenanceTbl, provenanceObjManager,
                                                                            aggregatorLock, self.MSDStat_Q,
                                                                            self.jobInfo_Q,)))
                # Aggregate OSS IO Stats into the Provenance Table
                procList.append(Process(target=self._aggregateFIO, args=(provenanceTbl, provenanceObjManager,
                                                                            aggregatorLock, self.OSSStat_Q,
                                                                            self.jobInfo_Q,)))
                # Aggregate File Operations into the Provenance Table
                procList.append(Process(target=self._aggregateFIO, args=(provenanceTbl, provenanceObjManager,
                                                                            aggregatorLock, self.fileOP_Q,
                                                                            self.jobInfo_Q,)))
                # Aggregate Job Info(s) from Job Scheduler(s)
                procList.append(Process(target=self._aggregateJobs, args=(jobScheduler, provenanceTbl,
                                                                          self.jobInfo_Q)))

                # Start all the aggregator processes:
                for proc in procList:
                    proc.daemon = True
                    proc.start()

                # Allow Processes to aggregate until time's up or shutdown is called
                time_keeper = self._interval
                while all(proc.is_alive() for proc in procList):
                    time_keeper -= 1
                    self.event_flag.wait(1)
                    if time_keeper <= 0:
                        break

                # stop the processes for this interval
                self.timesUp.set()

                # wait for all processes to finish
                for proc in procList:
                    if proc.is_alive():
                        proc.join()

                #------------------------- STORE DATA INTO DATABASE ---------------------------
                # Now dump the data into MonoDB
                procDBList: List[Process] = [
                    # Create a process to dump data into MongoDB
                    Process(target=MongoOPs.dump2MongoDB, args=(provenanceTbl.copy(),)),
                    # Create a process to dump time series data into InfluxDB
                    Process(target=InfluxOPs.dump2InfluxDB, args=(provenanceTbl.copy(), self.serverStats_Q,)),
                ]

                # Start all DB Process
                for procdb in procDBList:
                    procdb.start()

                # Wait for all Provenance Data Objects to be dumped into all databases
                for procdb in procDBList:
                    procdb.join()

                # -------------------- CLEAR EXCESSIVE DATA FROM MEMORY ------------------------
                # cleanup the ChangeLogs since they're already dumped into the database
                self._clearChangeLogs(provenanceTbl.copy())

                # Clear the Provenance Table to keep the memory usage optimized
                self._clearProvenTbl(provenanceTbl)

            # Terminate timer after flag is set
            #timer_flag.set()
            # jobScheduler.close()

        except JobSchedulerException as jobSchedExp:
            log(Mode.AGGREGATOR, jobSchedExp.getMessage())
            self.event_flag.set()

        except ProvenanceExitExp:
            pass

        finally:
            self.timesUp.set()
            provenanceObjManager.shutdown()

    #
    # Aggregate File OPs and IO stats
    #
    def _aggregateFIO(self, provenanceTbl: 'DictProxy', provObjMngr : '_AggregatorManager',
                         aggregatorLock, allFS_Q: Queue, jobInfo_Q : Queue):
        """
        Aggregate and map all the MDS/OSS/Changelog IO  data to a set of unique objects based on Job ID

        :param provenanceTbl: Multi-processing Manager Dict
        :param provObjMngr: Proxy Object of ProvenanceObj
        :param aggregatorLock: Process semaphor lock
        :param allFS_Q: Any multi-processing queue contains the MDS/OSS/Changelog
        :param jobInfo_Q: multi-processing queue of submitted jobs
        :return: None - Store data into a dictionoray of ProvenanceObjs
        """
        try:
            # Fill the provenanceTbl dictionary up until the interval time is up or server is shutting down
            while not any([self.timesUp.is_set(), self.shutdown.is_set()]):
                # all the MSDStat_Q, OSSStat_Q, and fileOP_Q are assumed as allFS_Q
                while not allFS_Q.empty():
                    # Finish the loop if time's up
                    if self.timesUp.is_set():
                        break
                    # Get the list of objects from each queue one by one
                    obj_List = allFS_Q.get()
                    # extract objects from obj_List which can be either MDSDataObj, OSSDataObj, or FileOpObj type
                    for obj_Q in obj_List:
                        # the uniqID function for each object in the queue should be the same for the same
                        # jobID, Cluster, and SchedType, not mather what type of object are they
                        uniq_id = obj_Q.uniqID()
                        # Ignore None hash IDs (i.e. Procs)
                        if uniq_id is None:
                            continue

                        # if no data has been collected for this JonID_Cluster_SchedType,
                        #  then create a ProvenanceObj and fill it
                        with aggregatorLock:
                            if not provenanceTbl.get(uniq_id, None):
                                provenanceTbl[uniq_id] = provObjMngr.ProvenanceObj()
                                # Only create jobID for JOB data
                                if obj_Q.jobid:
                                    # create an empty JobInfo object
                                    jobInfo = JobInfo()
                                    jobInfo.cluster = obj_Q.cluster
                                    jobInfo.sched_type = obj_Q.sched_type
                                    jobInfo.jobid = obj_Q.jobid
                                    jobInfo.taskid = (obj_Q.taskid if obj_Q.taskid else None)
                                    jobInfo.status = JobInfo.Status.NONE
                                    provenanceTbl._callmethod('__getitem__', (uniq_id,)).updateJobInfo(jobInfo)

                                    # Add a request in jobInfo_Q to get information from corresponding job scheduler
                                    job_info = '_'.join([obj_Q.cluster, obj_Q.sched_type, obj_Q.jobid +
                                                         ("." + obj_Q.taskid if obj_Q.taskid else "")])
                                    jobInfo_Q.put(job_info)

                        # Insert the corresponding object into its relevant list (sorted by timestamp)
                        # the _callmethod of Proxy class will take care of the complex object shared between processes
                        provenanceTbl._callmethod('__getitem__', (uniq_id,)).insert_sorted(obj_Q)

                # Wait if Queue is empty and check the Queue again
                self.timesUp.wait(1)

        except ProvenanceExitExp:
            pass

    #
    # Aggregate Job Info objects
    #
    def _aggregateJobs(self, jobScheduler: JobScheduler, provenanceTbl: 'DictProxy', jobInfo_Q : Queue):
        """
        Aggregate the Job Info objects that come from Job Scheduler(s)

        :param jobScheduler: JobScheduler Object
        :param provenanceTbl: Multi-processing Manager Dict
        :param jobInfo_Q: multi-processing queue of submitted jobs
        :param provObjMngr: Proxy Object of ProvenanceObj
        :param aggregatorLock: Process semaphor lock
        :return: None
        """
        # Get the JobInfo for each jobid only once in each round
        processed_jobids = set()
        # Fill the provenanceTbl dictionary up until the interval time is up
        try:
            while not any([self.timesUp.is_set(), self.shutdown.is_set()]):
                # Give it a second then check the jobInfo_Q again
                self.timesUp.wait(1)
                # Process all the jobs in the queue
                while not jobInfo_Q.empty():
                    # Finish the loop if time's up
                    if self.timesUp.is_set():
                        break

                    # Get aj JobInfo Request
                    job_req = jobInfo_Q.get()

                    # Do not get the JobInfo if it's already processed
                    if job_req in processed_jobids:
                        continue

                    # Unpack the job_req
                    cluster, sched, jobid = job_req.split('_')
                    taskid = None
                    if '.' in jobid:
                        jobid, taskid = jobid.split('.')

                    # Get Job Info from Job Scheduler
                    jobInfo : JobInfo = jobScheduler.getJobInfo(cluster, sched, jobid, taskid)
                    # Get the  JobInfo Unique ID
                    uniq_id = jobInfo.uniqID()

                    # If received a JobInfo that its uniq_id is not present in Provenance Table then ignore it
                    provenanceObj = provenanceTbl.get(uniq_id, None)
                    if not provenanceObj:
                        continue

                    # If job_script is empty then try to get the job script for this job
                    if not provenanceObj.jobInfo.job_script:
                        jobInfo.job_script = jobScheduler.getJobScript(jobInfo)

                    # In order to avoid keeping UNDEF JobInfos forever, we make sure they
                    # do not stay in memory more than 5 times in a row
                    if isinstance(provenanceObj.jobInfo, UGEJobInfo):
                        if provenanceObj.jobInfo.undef_cnt >= 5:
                            # If job has been stuck in UNDEF mode for 5 times in a row
                            # then we should assume the job is FINISHED (or no more available)
                            jobInfo.status = JobInfo.Status.FINISHED
                            # The job has been finished and no more data should be aggregated for this job
                            finishedJob = cluster + '_' + sched + '_' + str(jobid) + (
                                "." + str(taskid) if taskid else "")
                            finJobDB = FinishedJobs()
                            finJobDB.add(finishedJob)

                    # Update the JobInfo for this record in the Provenance Table
                    provenanceTbl._callmethod('__getitem__', (uniq_id,)).updateJobInfo(jobInfo)
                    # Check on this job again in next round if the status is not FINISHED yet
                    if jobInfo.status is not JobInfo.Status.FINISHED:
                        processed_jobids.add(job_req)

            # Once the time's up, put back all the processed JobIDs back into the queue for next round
            while processed_jobids:
                jobInfo_Q.put(processed_jobids.pop())

        except ProvenanceExitExp:
            pass


    #
    # Clear the ChangeLogs
    #
    def _clearChangeLogs(self, provenData: Dict[str, 'ProvenanceObj']):
        """
        Clear the changeLog records which are already dumped into the database
        that will help to keep the memory optimized

        :param provenData: Dict of ProvenanceObj
        :return: None
        """
        # Collect all the captured Changelog records along with their ids
        chlRecs = defaultdict(list)
        for provObj in provenData.values():
            for fopObj in provObj.FileOpObj_lst:
                chlRecs[fopObj.mdtTarget].append(fopObj.recID)
        # Clearing ChangeLogs per MDT, USER from the
        # beginning to the last record of current capture
        for mdtTarget, recIdList in chlRecs.items():
            if recIdList:
                # find the corresponding Changelog User of the given MDT
                user = self._chLogUsers[self._mdtTargets.index(mdtTarget)]
                # Get the last record that should be deleted
                endRec = max(recIdList)
                # clear the Changelog
                subprocess.check_output("lfs changelog_clear " + mdtTarget + " " + user + " " + str(endRec), shell=True)

    #
    # Clear the Provenance Table, but keep the uniq_id
    #
    def _clearProvenTbl(self, provenanceTbl: 'DictProxy'):
        """
            This method will clear the Provenance Table and keep the uniq_id keyes, since
            it assumes they will be removed along with their values after the correspoding
            job got finished.

        :param provenanceTbl: The Provenance Table Object
        :return: None
        """
        for uniq_id in provenanceTbl.keys():
            provenObj : ProvenanceObj = provenanceTbl.get(uniq_id)
            ProvenJobInfo: JobInfo = provenObj.jobInfo
            # Delete the record from Provenance Table if job is already finished
            if ProvenJobInfo:
                if ProvenJobInfo.status is JobInfo.Status.FINISHED:
                    provenanceTbl.pop(uniq_id)
                    continue
            # Otherwise, just reset the Provenance Object items
            provenanceTbl._callmethod('__getitem__', (uniq_id,)).reset()


    # Timer function to be used in a thread inside this process
    @staticmethod
    def _timer(timer_flag, timerIntv):
        while not timer_flag.is_set():
            global timer_val
            timer_val = time.time()
            print(" real time is: " + str(timer_val))
            time.sleep(timerIntv)


    #
    # Define a BaseManager Class to handle the Proxy Objects for
    #  Multiprocessing inside the aggregator
    #
    class _AggregatorManager(BaseManager):
        pass

    #
    # Internal Proxy Class
    #
    class _ProvenanceObjProxy(NamespaceProxy):
        """
        Define a Proxy Class to handle the ProvenanceObj object
        while it's being shared among multiple processes
        """
        # Specify which methods can be exposed to the outside
        _exposed_ = ('__getattribute__', '__setattr__', '__delattr__', 'insert_sorted', 'updateJobInfo', 'reset')

        # Create the proxy method that will be shared/called among multiple processes
        def updateJobInfo(self, jobInfo):
            # _callmethod returns the result of a method of the proxy’s referent
            callmethod = object.__getattribute__(self, '_callmethod')
            return callmethod(self.updateJobInfo.__name__, (jobInfo,))

        # Create the proxy method that will be shared/called among multiple processes
        def insert_sorted(self, value):
            # _callmethod returns the result of a method of the proxy’s referent
            callmethod = object.__getattribute__(self, '_callmethod')
            return callmethod(self.insert_sorted.__name__, (value,))

        # Create the proxy method that will be shared/called among multiple processes
        def reset(self):
            # _callmethod returns the result of a method of the proxy’s referent
            callmethod = object.__getattribute__(self, '_callmethod')
            callmethod(self.reset.__name__, ())

#
# The main Provenance Object that holds the collected/aggregated data
# from distributed servers and services
#
class ProvenanceObj(object):
    """
    The most important Provenance Object that holds the final aggregated form of collected data
    before it gets dumpped into a database
    """
    def __init__(self):
        self.jobInfo : [JobInfo] = None
        self.MDSDataObj_lst: List[MDSDataObj] = []
        self.OSSDataObj_lst: List[OSSDataObj] = []
        self.FileOpObj_lst: List[FileOpObj] = []
        self.ignore_file_fids: Set[str] = set()
        self.__MDSDataObj_keys: List[float] = []
        self.__OSSDataObj_keys: List[float] = []
        self.__FileOpObj_keys: List[float] = []

    #
    # update the jobInfo object
    def updateJobInfo(self, jobinfo):
        if type(self.jobInfo) is not type(jobinfo):
            self.jobInfo = jobinfo
        else:
            self.jobInfo.update(jobinfo)

    # This function receives any type of MDSDataObj, OSSDataObj, or FileOpObj
    # objects and uses the timestamp attribute as the key to sort them while
    # appending them their corresponding list
    def insert_sorted(self, dataObj):
        # select timestamp as the key
        key = float(dataObj.timestamp)
        # define the corresponding list
        if isinstance(dataObj, MDSDataObj):
            targetList = self.MDSDataObj_lst
            keyList = self.__MDSDataObj_keys
        elif isinstance(dataObj, OSSDataObj):
            targetList = self.OSSDataObj_lst
            keyList = self.__OSSDataObj_keys
        elif isinstance(dataObj, FileOpObj):
            targetList = self.FileOpObj_lst
            keyList = self.__FileOpObj_keys
            self._filter_and_ignore(dataObj)
        else:
            raise AggregatorException("[ProvenanceObj] Wrong instance of an object in the Queue")

        # determine where to store index based on timestamps
        inx = bisect_left(keyList, key)
        # insert the key for future sort
        keyList.insert(inx, key)
        # Insert the dataObj in a sorted list
        targetList.insert(inx, dataObj)

    # Reset everything at some point to keep the memory usage optimized
    # We can still keep the JobInfo data since they do not hold lot of space
    def reset(self):
        self.MDSDataObj_lst.clear()
        self.OSSDataObj_lst.clear()
        self.FileOpObj_lst.clear()
        self.__MDSDataObj_keys.clear()
        self.__OSSDataObj_keys.clear()
        self.__FileOpObj_keys.clear()

    # Find the last MDS object (sorted by time) per MDS and MDT
    # Return a dictionary
    def get_MDS_table(self) -> Dict:
        mdsObjTbl = {}
        for mdsData in self.MDSDataObj_lst:
            # Add this mds to Table if it does not exist
            if mdsData.mds_host not in mdsObjTbl.keys():
                mdsObjTbl[mdsData.mds_host] = {}

            # Add this mdt for this mds if it does not exist
            if mdsData.mdt_target not in mdsObjTbl[mdsData.mds_host].keys():
                mdsObjTbl[mdsData.mds_host][mdsData.mdt_target] = mdsData
                continue

            # Get the Previous data of this MDT/MDS
            mdsObj = mdsObjTbl[mdsData.mds_host][mdsData.mdt_target]

            # Update the object if the snapshot time is changed
            if mdsData.snapshot_time != mdsObj.snapshot_time:
                # Adjust the following properties
                attrs = ["open", "close", "mknod", "link", "unlink", "mkdir", "rmdir", "rename",
                         "getattr", "setattr", "samedir_rename", "crossdir_rename"]
                for attr in attrs:
                    mdsData_val = getattr(mdsData, attr)
                    mdsObj_val = getattr(mdsObj, attr)
                    setattr(mdsData, attr, (mdsData_val + mdsObj_val))

                # Update the mdsObjTbl with current adjusted mdsData
                mdsObjTbl[mdsData.mds_host][mdsData.mdt_target] = mdsData

        # Return mdsObjTbl
        return mdsObjTbl

    # Aggregate all the OSS object data and put them in a table based on OST/OSS
    # Return a dictionary
    def get_OSS_table(self) -> Dict:
        ossObjTbl = {}  # {oss : {ost : ossObj}}
        for ossData in self.OSSDataObj_lst:
            # Add this oss to Table if it does not exist
            if ossData.oss_host not in ossObjTbl.keys():
                ossObjTbl[ossData.oss_host] = {}

            # Add this ost for this oss if does not exist
            if ossData.ost_target not in ossObjTbl[ossData.oss_host].keys():
                ossObjTbl[ossData.oss_host][ossData.ost_target] = ossData
                continue

            # Get the Previous data of this OST/OSS
            ossObj = ossObjTbl[ossData.oss_host][ossData.ost_target]

            # Update the object if the snapshot time is changed
            if ossData.snapshot_time != ossObj.snapshot_time:
                # Adjust read_byte data
                ossData.read_bytes_min = min(ossData.read_bytes_min, ossObj.read_bytes_min)
                ossData.read_bytes_max = max(ossData.read_bytes_max, ossObj.read_bytes_max)
                # Adjust write_byte data
                ossData.write_bytes_min = min(ossData.write_bytes_min, ossObj.write_bytes_min)
                ossData.write_bytes_max = max(ossData.write_bytes_max, ossObj.write_bytes_max)
                # Adjust the rest of the properties
                attrs = ["read_bytes", "write_bytes", "read_bytes_sum", "write_bytes_sum",
                         "getattr", "setattr", "punch", "sync", "destroy", "create"]
                for attr in attrs:
                    ossData_val = getattr(ossData, attr)
                    ossObj_val = getattr(ossObj, attr)
                    setattr(ossData, attr, (ossData_val + ossObj_val))

                # Update the ossObjTbl with current adjusted ossData
                ossObjTbl[ossData.oss_host][ossData.ost_target] = ossData

        # Return the table
        return ossObjTbl

    #
    # Add to the list of to-be-ignored files that may not need to be
    # stored in the database
    def _filter_and_ignore(self, fileObj):
        if fileObj.target_file and re.match(r"^\.job_finished\.\d+", fileObj.target_file):
            self.ignore_file_fids.add(fileObj.target_fid)



#
# In case of error the following exception can be raised
#
class AggregatorException(Exception):
    def __init__(self, message):
        super(AggregatorException, self).__init__(message)
        self.message = "\n [Error] _AGGREGATOR_: " + message + "\n"

    def getMessage(self):
        return self.message

# -*- coding: utf-8 -*-
"""
    The "aggregator" module containes the main Aggregator class which receives the collected data from agents
    and aggregate them all into a comprehensive and meaningful data to be stored in database or used as a query
    data for the Provenance API

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from multiprocessing.managers import BaseManager, NamespaceProxy, DictProxy
from scheduler import JobScheduler, JobInfo, JobSchedulerException
from multiprocessing import Process, Event, Manager, Lock, Queue
from config import ServerConfig, ConfigReadExcetion
from threading import Event as Event_Thr, Thread
from file_io_stats import MDSDataObj, OSSDataObj
from exceptions import ProvenanceExitExp
from collections import defaultdict
from db_operations import MongoOPs
from file_op_logs import FileOpObj
from bisect import bisect_left
from tabulate import tabulate
from typing import List, Dict
from logger import log, Mode
import subprocess
import time, os

#------ Global Variable ------
# Timer Value
timer_val = 0.0

class Aggregator(Process):
    """
    Aggregate all MDS, OSS and File Operation data collected from different targets
    and store them into various databases.
    """
    def __init__(self, MSDStat_Q, OSSStat_Q, fileOP_Q):
        Process.__init__(self)
        self.MSDStat_Q = MSDStat_Q
        self.OSSStat_Q = OSSStat_Q
        self.fileOP_Q = fileOP_Q
        self.jobInfo_Q = Queue()
        self.event_flag = Event()
        self.timesUp = Event()
        self.currentTime = 0
        self.config = ServerConfig()
        try:
            self._interval = self.config.getAggrIntv()
            self._timerIntv = self.config.getAggrTimer()
            self._mdtTargets = self.config.getMdtTargets()
            self._chLogUsers = self.config.getChLogsUsers()

            self._jobScheduler = JobScheduler()

        except ConfigReadExcetion as confExp:
            log(Mode.AGGREGATOR, confExp.getMessage())
            self.event_flag.set()

        except JobSchedulerException as jobSchedExp:
            log(Mode.AGGREGATOR, jobSchedExp.getMessage())
            self.event_flag.set()


    # Implement Process.run()
    def run(self):
        # Register the ProvenanceObj to the Aggregator Manager along with the Proxy class
        self._AggregatorManager.register('ProvenanceObj', ProvenanceObj, self._ProvenanceObjProxy)
        # Start the Base manager
        provenanceObjManager = self._AggregatorManager()
        provenanceObjManager.start()

        try:
            # Create a Timer Thread to be running for this process and change the
            # "timer_val" value every
            timer_flag = Event_Thr()
            timer = Thread(target=self._timer, args=(timer_flag, self._timerIntv,))
            timer.setDaemon(True)
            #==timer.start()

            # Create a shard Dictionary object which allows three process to update their values
            provFSTbl = Manager().dict()

            while not self.event_flag.is_set():
                # Clear the Provenance Table after each round (Keep the Memory usage Optimized)
                provFSTbl.clear()
                # Record the current timestamp
                self.currentTime = time.time()
                # a list of Processes
                procList: List[Process] = []
                # Create a shard Dictionary object which allows three process to update their values
                #===provFSTbl = Manager().dict()
                # Manage critical sections
                aggregatorLock = Lock()
                # reset the Times Up signal for all process
                self.timesUp.clear()
                #---------------------- AGGREGATE DATA -------------------------------
                # Aggregate MDS IO Stats into the Provenance Table
                procList.append(Process(target=self._aggregateFIO, args=(provFSTbl, provenanceObjManager,
                                                                            aggregatorLock, self.MSDStat_Q,
                                                                            self.jobInfo_Q,)))
                # Aggregate OSS IO Stats into the Provenance Table
                procList.append(Process(target=self._aggregateFIO, args=(provFSTbl, provenanceObjManager,
                                                                            aggregatorLock, self.OSSStat_Q,
                                                                            self.jobInfo_Q,)))
                # Aggregate File Operations into the Provenance Table
                procList.append(Process(target=self._aggregateFIO, args=(provFSTbl, provenanceObjManager,
                                                                            aggregatorLock, self.fileOP_Q,
                                                                            self.jobInfo_Q,)))
                # Aggregate Job Info(s) from Job Scheduler(s)
                procList.append(Process(target=self._aggregateJobs, args=(self._jobScheduler, provFSTbl,
                                                                          self.jobInfo_Q, provenanceObjManager,
                                                                            aggregatorLock,)))

                # Start all the aggregator processes:
                for proc in procList:
                    proc.daemon = True
                    proc.start()

                # Keep the time interval
                self.event_flag.wait(self._interval)
                # stop the processes for this interval
                self.timesUp.set()

                # wait for all processes to finish
                for proc in procList:
                    proc.join()

                #------------------------- STORE DATA INTO DATABASE ---------------------------
                # Now dump the data into MonoDB
                procDBList: List[Process] = [
                    # Create a separate process for MongoDB
                    Process(target=MongoOPs.dump2MongoDB, args=(provFSTbl.copy(),))
                ]

                # Start all DB Process
                for procdb in procDBList:
                    procdb.start()

                # Wait for all Provenance Data Objects to be dumped into all databases
                for procdb in procDBList:
                    procdb.join()

                # Now cleanup the ChangeLogs since they're already dumped into the database
                self._clearChangeLogs(provFSTbl.copy())

            # Terminate timer after flag is set
            timer_flag.set()

        except ProvenanceExitExp:
            pass

        finally:
            self.timesUp.set()
            provenanceObjManager.shutdown()

    #
    # Aggregate File OPs and IO stats
    #
    def _aggregateFIO(self, provFSTbl: 'DictProxy', provObjMngr : '_AggregatorManager',
                         aggregatorLock, allFS_Q: Queue, jobInfo_Q : Queue):
        """
        Aggregate and map all the MDS/OSS/Changelog IO  data to a set of unique objects based on Job ID

        :param provFSTbl: Multi-processing Manager Dict
        :param provObjMngr: Proxy Object of ProvenanceObj
        :param aggregatorLock: Process semaphor lock
        :param allFS_Q: Any multi-processing queue contains the MDS/OSS/Changelog
        :param jobInfo_Q: multi-processing queue of submitted jobs
        :return: None - Store data into a dictionoray of ProvenanceObjs
        """
        try:
            # Fill the provFSTbl dictionary up until the interval time is up
            while not self.timesUp.is_set():
                # all the MSDStat_Q, OSSStat_Q, and fileOP_Q are assumed as allFS_Q
                if not allFS_Q.empty():
                    #print(provFSTbl)
                    # Get the list of objects from each queue one by one
                    obj_List = allFS_Q.get()
                    # extract objects from obj_List which can be either MDSDataObj, OSSDataObj, or FileOpObj type
                    for obj_Q in obj_List:
                        # the uniqID function for each object in the queue should be the same for the same
                        # jobID, Cluster, and SchedType, not mather what type of object are they
                        uniq_id = obj_Q.uniqID()
                        #print(uniq_id)
                        # Ignore None hash IDs (i.e. Procs)
                        if uniq_id is None:
                            continue
                        # if no data has been collected for this JonID_Cluster_SchedType,
                        #  then create a ProvenanceObj and fill it
                        with aggregatorLock:
                            if not provFSTbl.get(uniq_id):
                                provFSTbl[uniq_id] = provObjMngr.ProvenanceObj()
                                # create an empty JobInfo object
                                jobInfo = JobInfo()
                                jobInfo.cluster = obj_Q.cluster
                                jobInfo.sched = obj_Q.sched_type
                                jobInfo.jobid = obj_Q.jobid
                                jobInfo.taskid = (obj_Q.taskid if obj_Q.taskid else None)
                                jobInfo.status = JobInfo.Status.NONE
                                provFSTbl._callmethod('__getitem__', (uniq_id,)).updateJobInfo(jobInfo)

                        # Add a request in jobInfo_Q to get information from corresponding job scheduler
                        job_info = '_'.join([obj_Q.cluster, obj_Q.sched_type, obj_Q.jobid +
                                             ("." + obj_Q.taskid if obj_Q.taskid else "")])
                        jobInfo_Q.put(job_info)

                        # Insert the corresponding object into its relevant list (sorted by timestamp)
                        # the _callmethod of Proxy class will take care of the complex object shared between processes
                        provFSTbl._callmethod('__getitem__', (uniq_id,)).insert_sorted(obj_Q)

                # Wait if Queue is empty and check the Queue again
                #print(provFSTbl)
                self.timesUp.wait(1)

        except ProvenanceExitExp:
            pass

    #
    # Aggregate Job Info objects
    #
    def _aggregateJobs(self, jobScheduler: JobScheduler, provFSTbl: 'DictProxy', jobInfo_Q : Queue,
                       provObjMngr : '_AggregatorManager', aggregatorLock):
        """
        Aggregate the Job Info objects that come from Job Scheduler(s)

        :param jobScheduler: JobScheduler Object
        :param provFSTbl: Multi-processing Manager Dict
        :param jobInfo_Q: multi-processing queue of submitted jobs
        :param provObjMngr: Proxy Object of ProvenanceObj
        :param aggregatorLock: Process semaphor lock
        :return: None
        """
        # Fill the provFSTbl dictionary up until the interval time is up
        while not self.timesUp.is_set():
            # all the MSDStat_Q, OSSStat_Q, and fileOP_Q are assumed as allFS_Q
            if not jobInfo_Q.empty():
                # Get aj JobInfo Request
                job_req = jobInfo_Q.get()
                # Unpack the job_req
                cluster, sched, jobid = job_req.split('_')
                taskid = None
                if '.' in jobid:
                    jobid, taskid = jobid.split('.')

                # Get Job Info from Job Scheduler
                jobInfo : JobInfo = jobScheduler.getJobInfo(cluster, sched, jobid, taskid)
                # Get the  JobInfo Unique ID
                uniq_id = jobInfo.uniqID()
                print(f"---------- JobInfo: req:[{job_req}]  {jobInfo.jobid}  {jobInfo.status} -------")
                # try:
                #     if provFSTbl._callmethod('__getitem__', (uniq_id,)).jobInfo.status \
                #             is not JobInfo.Status.FINISHED:
                #         # Update the corresponding object in Provenance Table with this JobInfo object
                #         provFSTbl._callmethod('__getitem__', (uniq_id,)).updateJobInfo(jobInfo)
                # except KeyError:
                #     print(f"------ keyError {uniq_id} ------ ")
                #     jobInfo_Q.put(job_req)
                if not provFSTbl.get(uniq_id):
                    with aggregatorLock:
                        provFSTbl[uniq_id] = provObjMngr.ProvenanceObj()

                provFSTbl._callmethod('__getitem__', (uniq_id,)).updateJobInfo(jobInfo)



            self.timesUp.wait(1)

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



    @staticmethod
    def _tableView(provFSTbl : 'DictProxy') -> None:
        provDict = provFSTbl._getvalue()
        ptable = []
        jobAttrs = []
        mdsAttrs = []
        ossAttrs = []
        fopAttrs = []

        # find last object
        provenObj = None
        for obj in provDict.values():
            if provenObj is None:
                provenObj = obj
                continue

            if obj.jobInfo.jobid and provenObj.jobInfo.jobid:
                if int(obj.jobInfo.jobid) > int(provenObj.jobInfo.jobid):
                    provenObj = obj

        jobInfo = provenObj.jobInfo
        mdsObj = provenObj.MDSDataObj_lst[-1] if provenObj.MDSDataObj_lst else None
        ossobj = provenObj.OSSDataObj_lst[-1] if provenObj.OSSDataObj_lst else None
        fopObj = provenObj.FileOpObj_lst[-1] if provenObj.FileOpObj_lst else None

        if jobInfo:
            jobAttrs = [atr for atr in dir(jobInfo) if (not atr.startswith('__')) and (not callable(getattr(jobInfo, atr)))]

        if mdsObj:
            mdsAttrs = [atr for atr in dir(mdsObj) if (not atr.startswith('__')) and (not callable(getattr(mdsObj, atr)))]

        if ossobj:
            ossAttrs = [atr for atr in dir(ossobj) if (not atr.startswith('__')) and (not callable(getattr(ossobj, atr)))]

        if fopObj:
            fopAttrs = [atr for atr in dir(fopObj) if (not atr.startswith('__')) and (not callable(getattr(fopObj, atr)))]

        # find largest list
        maxAttsLen = max([len(jobAttrs), len(mdsAttrs), len(ossAttrs), len(fopAttrs)])
        allAttrObjs = [jobAttrs, mdsAttrs, ossAttrs, fopAttrs]
        allObjs = [jobInfo, mdsObj, ossobj, fopObj]
        # Create pTable
        for inx in range(maxAttsLen):
            record = []
            in_inx = 0
            for objAttrs in allAttrObjs:
                if inx < len(objAttrs):
                    record.extend([objAttrs[inx], str(getattr(allObjs[in_inx], objAttrs[inx])), "*"])
                else:
                    record.extend(["", "", "*"])
                in_inx += 1
            ptable.append(record[:-1])

        os.system("reset && printf '\e[3J'")
        print(tabulate(ptable, headers=["Job Info:", "Value:", "*", "MDS:", "Value:", "*", "OSS:", "Value:", "*",
                                        "File OP:", "Value:"], tablefmt="fancy_grid"))


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
        _exposed_ = ('__getattribute__', '__setattr__', '__delattr__', 'insert_sorted', 'updateJobInfo')

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
        self.__MDSDataObj_keys: List[float] = []
        self.__OSSDataObj_keys: List[float] = []
        self.__FileOpObj_keys: List[float] = []

    #
    # update the jobInfo object
    def updateJobInfo(self, jobInfo):
        self.jobInfo = jobInfo
        self.jobInfo.jobid = int(jobInfo.jobid)

    # This function receives any type of MDSDataObj, OSSDataObj, or FileOpObj
    # objects and uses the timestamp attribute as the key to sort them while
    # appending them their corresponding list
    def insert_sorted(self, dataObj):
        self.jobid = dataObj.jobid
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
        else:
            raise AggregatorException("[ProvenanceObj] Wrong instance of an object in the Queue")

        # determine where to store index based on timestamps
        inx = bisect_left(keyList, key)
        # insert the key for future sort
        keyList.insert(inx, key)
        # Insert the dataObj in a sorted list
        targetList.insert(inx, dataObj)


#
# In case of error the following exception can be raised
#
class AggregatorException(Exception):
    def __init__(self, message):
        super(AggregatorException, self).__init__(message)
        self.message = "\n [Error] _AGGREGATOR_: " + message + "\n"

    def getMessage(self):
        return self.message


#for attr in [atr for atr in dir(fsIOObj) if not atr.startswith('__')]:
#    print(attr + " --> " + str(getattr(fsIOObj, attr)))
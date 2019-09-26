"""
    The "aggregator" module containes the main Aggregator class which receives the collected data from agents
    and aggregate them all into a comprehensive and meaningful data to be stored in database or used as a query
    data for the Provenance API

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
from multiprocessing import Process, Event, Manager
from threading import Event as Event_Thr, Thread
from file_io_stats import MDSDataObj, OSSDataObj
from typing import Dict, Sequence, List
from file_op_logs import FileOpObj
from bisect import bisect_left
import time

#------ Global Variable ------
# Timer Value
timer_val = 0.0

class Aggregator(Process):

    def __init__(self, MSDStat_Q, OSSStat_Q, fileOP_Q):
        Process.__init__(self)
        self.MSDStat_Q = MSDStat_Q
        self.OSSStat_Q = OSSStat_Q
        self.fileOP_Q = fileOP_Q
        self.event_flag = Event()
        self.timesUp = Event()
        self.config = ServerConfig()
        try:
            self.__interval = self.config.getAggrIntv()
            self.__timerIntv = self.config.getAggrTimer()

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())
            self.event_flag.set()

    # Implement Process.run()
    def run(self):
        # Create a Timer Thread to be running for this process and change the
        # "timer_val" value every
        timer_flag = Event_Thr()
        timer = Thread(target=self.__timer, args=(timer_flag, self.__timerIntv,))
        timer.setDaemon(True)
        #==timer.start()

        while not self.event_flag.is_set():
            print("========= Start aggregating:")
            # a list of Processes
            procList: List[Process] = []
            # Create a shard Dictionary object which allows three process to update their values
            provFSTbl = Manager().dict()
            # reset the Times Up signal for all process
            self.timesUp.clear()
            # Aggregate MDS IO Stats into the Provenance Table
            procList.append(Process(target=self.__aggregate2Dict, args=(provFSTbl, self.MSDStat_Q,)))
            # Aggregate OSS IO Stats into the Provenance Table
            procList.append(Process(target=self.__aggregate2Dict, args=(provFSTbl, self.OSSStat_Q,)))
            # Aggregate File Operations into the Provenance Table
            procList.append(Process(target=self.__aggregate2Dict, args=(provFSTbl, self.fileOP_Q,)))
            # Start all the aggregator processes:
            for proc in procList:
                proc.start()
            # Keep the time interval
            self.event_flag.wait(self.__interval)
            # stop the processes for this interval
            self.timesUp.set()
            # wait for all processes to finish
            for proc in procList:
                proc.join()
            print("========= End aggregating:")
            if provFSTbl:
                for key, valuObj in provFSTbl.items():
                    #print(" key id: " + str(key))
                    #print(" --MDS_keys:" + str(valuObj.__MDSDataObj_keys))
                    #print(" --OSS_keys:" + str(valuObj.__OSSDataObj_keys))
                    #print(" --FileOps_keys:" + str(valuObj.FileOpObj_keys))
                    pass

        # Terminate timer after flag is set
        timer_flag.set()

    # Aggregate and map all the MDS IO status data to a set of unique objects based on Job ID
    def __aggregate2Dict(self, provFSTbl: Dict, allFS_Q):
        # Fill the provFSTbl dictionary up until the interval time is up
        while not self.timesUp.is_set():
            # all the MSDStat_Q, OSSStat_Q, and fileOP_Q are assumed as allFS_Q
            if not allFS_Q.empty():
                print(provFSTbl)
                # Get the list of objects from each queue one by one
                obj_List = allFS_Q.get()
                # extract objects from obj_List which can be either MDSDataObj, OSSDataObj, or FileOpObj type
                for obj_Q in obj_List:
                    # the hash function for each object in the queue should be the same for the same
                    # jobID, Cluster, and SchedType, not mather what type of object are they
                    hash_id = hash(obj_Q)
                    # Ignore None hash IDs
                    if hash_id is None:
                        continue
                    # if no data has been entered for this JonID_Cluster_SchedType, then create a ProvenanceObj
                    # and fill it, otherwise append the object to the current available ProvenanceObj
                    if not hash_id in provFSTbl.keys():
                        provFSTbl[hash_id] = ProvenanceObj()

                    #Insert the corresponding object into its relevant list (sorted by timestamp)
                    # -- This looks naive but the Manager().Dict() behaves weird when you want to
                    # -- operate on its objects. The only way it works is this:
                    provenanceObj = provFSTbl[hash_id]
                    provenanceObj.insert_sorted(obj_Q)
                    provFSTbl[hash_id] = provenanceObj

            # Wait if Queue is empty and check the Queue again
            print(provFSTbl)
            self.timesUp.wait(1)



    # Timer function to be used in a thread inside this process
    @staticmethod
    def __timer(timer_flag, timerIntv):
        while not timer_flag.is_set():
            global timer_val
            timer_val = time.time()
            print(" real time is: " + str(timer_val))
            time.sleep(timerIntv)


class ProvenanceObj(object):
    def __init__(self):
        self.MDSDataObj_lst: List[MDSDataObj] = []
        self.OSSDataObj_lst: List[OSSDataObj] = []
        self.FileOpObj_lst: List[FileOpObj] = []
        self.__MDSDataObj_keys: List[float] = []
        self.__OSSDataObj_keys: List[float] = []
        self.__FileOpObj_keys: List[float] = []

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

        else:
            raise AggregatorException("Wrong instance of an object in the Queue")

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
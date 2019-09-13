"""
    The "aggregator" module containes the main Aggregator class which receives the collected data from agents
    and aggregate them all into a comprehensive and meaningful data to be stored in database or used as a query
    data for the Provenance API

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
from multiprocessing import Process, Event, Manager
from file_io_stats import MDSDataObj, OSSDataObj
from threading import Event as Event_Thr, Thread
from typing import Dict, Sequence
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
        # Define a new Dictionary type: {<int>, <ProvenanceObj>}
        ProvenanceType = Dict[int, ProvenanceObj]
        #
        # Create a Timer Thread to be running for this process and change the
        # "timer_val" value every
        timer_flag = Event_Thr()
        timer = Thread(target=self.__timer, args=(timer_flag, self.__timerIntv,))
        timer.setDaemon(True)
        #==timer.start()

        while not self.event_flag.is_set():
            # a list of Processes
            procList = [Process]
            # Create a shard Dictionary object which allows three process to update their values
            jobFSTbl = Manager().dict(Sequence[ProvenanceType])
            # reset the Times Up signal for all process
            self.timesUp.clear()

            #

            while not self.filesystem_Q.empty():
                fsIOObj = self.filesystem_Q.get()
                # MDS or OSS data?
                if isinstance(fsIOObj, MDSDataObj):
                    print("========== MDS: " + fsIOObj.mds_host + " ============")
                    for attr in [atr for atr in dir(fsIOObj) if not atr.startswith('__')]:
                        print(attr + " --> " + str(getattr(fsIOObj, attr)))

                elif isinstance(fsIOObj, OSSDataObj):
                    print("========== OSS: " + fsIOObj.oss_host + " ============")
                    for attr in [atr for atr in dir(fsIOObj) if not atr.startswith('__')]:
                        print(attr + " --> " + str(getattr(fsIOObj, attr)))
                else:
                    raise AggregatorException("Wrong 'fsIOObj' instance")

            # Interval to wait when queue is empty
            self.event_flag.wait(self.__interval)

        # Terminate timer after flag is set
        timer_flag.set()

    # Aggregate and map all the MDS IO status data to a set of unique objects based on Job ID
    def __aggrMDSioStats(self):
        pass

    # Timer function to be used in a thread inside this process
    @staticmethod
    def __timer(timer_flag, timerIntv):
        while not timer_flag.is_set():
            global timer_val
            timer_val = time.time()
            print(" real time is: " + str(timer_val))
            time.sleep(timerIntv)


class ProvenanceObj:
    def __init__(self):
        self.jobid = None
        self.cluster = None
        self.sched_type = None
        self.mds_host = None



#
# In case of error the following exception can be raised
#
class AggregatorException(Exception):
    def __init__(self, message):
        super(AggregatorException, self).__init__(message)
        self.message = "\n [Error] _AGGREGATOR_: " + message + "\n"

    def getMessage(self):
        return self.message


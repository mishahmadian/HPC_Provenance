"""
    The "aggregator" module containes the main Aggregator class which receives the collected data from agents
    and aggregate them all into a comprehensive and meaningful data to be stored in database or used as a query
    data for the Provenance API

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
from file_io_stats import MDSDataObj, OSSDataObj
from multiprocessing import Process, Event
from threading import Timer, Lock

class Aggregator(Process):
    def __init__(self, fsIOstat_Q):
        Process.__init__(self)
        self.fsIOstat_Q = fsIOstat_Q
        self.event_flag = Event()
        self.config = ServerConfig()

    # Implement Process.run()
    def run(self):
        while not self.event_flag.is_set():
            print("==========  ENTER  ============")
            while not self.fsIOstat_Q.empty():
                fsIOObj = self.fsIOstat_Q.get()
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
            waitIntv = self.config.getAggrIntv()
            self.event_flag.wait(waitIntv)

        # Terminate itself after flag is set
        self.terminate()


            


#
# In case of error the following exception can be raised
#
class AggregatorException(Exception):
    def __init__(self, message):
        super(AggregatorException, self).__init__(message)
        self.message = "\n [Error] _AGGREGATOR_: " + message + "\n"

    def getMessage(self):
        return self.message


# -*- coding: utf-8 -*-
"""
    The main module runs on the Provenance Server machine. This module
    executes of several concurrent tasks:

        1. Receiving I/O statistic data from Provenance agent on Lustre servers
        2. Collecting Lustre Changelog data from the client side
        3. Aggregate the data from 1 & 2 in a meaningful way
        4. Store data on Database

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from file_io_stats import IOStatsListener, IOStatsException
from aggregator import Aggregator, AggregatorException
from db_manager import MongoDB, DBManagerException
from multiprocessing import Process, Queue, Event
from file_op_logs import ChangeLogCollector
from communication import CommunicationExp
from exceptions import ProvenanceExitExp
from config import ConfigReadExcetion
from persistant import FinishedJobs
from logger import log, Mode
from time import sleep, time
from typing import List
import signal

#
# The main class which is executed by the main Daemon process
#
class Main_Interface:
    def __init__(self):
        try:
            self._processlist : List[Process] = []
            self.MSDStat_Q = Queue()
            self.OSSStat_Q = Queue()
            self.fileOP_Q = Queue()
            self.serverStats_Q = Queue()
            self.shut_down = Event()

            # Register signal handler
            signal.signal(signal.SIGINT, self.server_exit)
            signal.signal(signal.SIGTERM, self.server_exit)
            signal.signal(signal.SIGHUP, self.server_exit)

        except OSError:
            pass

    # Handle the SIGINT and SIGTERM signals in order to shutdown the server
    def server_exit(self, sig, frame):
        raise ProvenanceExitExp

    # Main Function
    def run_server(self):
        try:
            #
            # Prepare Databases
            mongoDB = MongoDB()
            mongoDB.init()
            mongoDB.close()

            # Initialize FinishedJobs File
            finJobsDB = FinishedJobs()
            finJobsDB.init()

            # IO Stats Listener Process
            IOStatsLsn_Proc = IOStatsListener(self.MSDStat_Q, self.OSSStat_Q, self.serverStats_Q, self.shut_down)
            self._processlist.append(IOStatsLsn_Proc)

            # File Operation Log collector Process
            fileOPStats_Proc = ChangeLogCollector(self.fileOP_Q, self.shut_down)
            self._processlist.append(fileOPStats_Proc)

            # Aggregator Process
            aggregator_Proc = Aggregator(self.MSDStat_Q, self.OSSStat_Q, self.fileOP_Q,
                                         self.serverStats_Q, self.shut_down)
            self._processlist.append(aggregator_Proc)

            # Start all Modules
            for proc in self._processlist:
                proc.start()

            log(Mode.APP_START, "***************** Provenance Server Started *****************")

            # keep the main process alive ans keep monitoring the Modules
            while not self.shut_down.is_set():
                # Periodically check on processes and make sure they're running fine,
                # otherwise kill the server process
                if not all(process.is_alive() for process in self._processlist):
                    log(Mode.MAIN, "[Process Dead] One or some of the module processes have been terminated")
                    raise ProvenanceExitExp
                sleep(1)

        except ProvenanceExitExp:
            # Shutdown timeout
            timeout = 10
            start_time = time()
            # Signal all the processes to shutdown
            self.shut_down.set()
            print(f"Provenance Server is shutting down ", end='', flush=True)
            # Allow all process for a few seconds to finish
            while time() - start_time <= timeout:
                print(".", end='', flush=True)
                if not any(process.is_alive() for process in self._processlist):
                    # All the processes are finished
                    break
                sleep(1)
            else:
                # If time is out and still a process is not terminated, then kill it
                for process in self._processlist:
                    if process.is_alive():
                        process.terminate()
                        process.join()

            print(" Done.")

            log(Mode.APP_EXIT, "***************** Provenance Server Stopped *****************")

        except ConfigReadExcetion as confExp:
            log(Mode.MAIN, confExp.getMessage())

        except CommunicationExp as commExp:
            log(Mode.MAIN, commExp.getMessage())

        except IOStatsException as iostExp:
            log(Mode.MAIN, iostExp.getMessage())

        except AggregatorException as aggrExp:
            log(Mode.MAIN, aggrExp.getMessage())

        except DBManagerException as dbExp:
            log(Mode.MAIN, dbExp.getMessage())

        except Exception as exp:
            log(Mode.MAIN, str(exp))

#
# Main
#
if __name__ == "__main__":
    mainInterface = Main_Interface()
    mainInterface.run_server()

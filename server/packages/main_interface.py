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
from file_op_logs import ChangeLogCollector
from communication import CommunicationExp
from exceptions import ProvenanceExitExp
from config import ConfigReadExcetion
from persistant import FinishedJobs
from multiprocessing import Queue
from logger import log, Mode
from time import sleep
import signal

#
# The main class which is executed by the main Daemon process
#
class Main_Interface:
    def __init__(self):
        self.IOStatsLsn_Proc = None
        self.fileOPStats_Proc = None
        self.aggregator_Proc = None
        self.count = 0

        self.MSDStat_Q = Queue()
        self.OSSStat_Q = Queue()
        self.fileOP_Q = Queue()

        # Register signal handler
        signal.signal(signal.SIGINT, self.server_exit)
        signal.signal(signal.SIGTERM, self.server_exit)

    # Handle the SIGINT and SIGTERM signals in order to shutdown the server
    def server_exit(self, sig, frame):
        raise ProvenanceExitExp

    # Main Function
    def run_server(self):
        try:
            log(Mode.APP_START, "***************** Provenance Server Started *****************")
            #
            # Prepare Databases
            mongoDB = MongoDB()
            mongoDB.init()
            mongoDB.close()
            # Initialize FinishedJobs File
            finJobsDB = FinishedJobs()
            finJobsDB.init()
            # IO Stats Listener Process
            self.IOStatsLsn_Proc = IOStatsListener(self.MSDStat_Q, self.OSSStat_Q)
            #self.IOStatsLsn_Proc.daemon = True
            self.IOStatsLsn_Proc.start()
            #self.IOStatsLsn_Proc.join()

            # File Operation Log collector Process
            self.fileOPStats_Proc = ChangeLogCollector(self.fileOP_Q)
            self.fileOPStats_Proc.start()

            # Aggregator Process
            self.aggregator_Proc = Aggregator(self.MSDStat_Q, self.OSSStat_Q, self.fileOP_Q)
            self.aggregator_Proc.start()
            #self.aggregator_Proc.join()

            while True:
                sleep(0.5)
                if not self.fileOPStats_Proc.is_alive():
                    log(Mode.MAIN, "[Process Dead] The file_op_logs process has been terminated")
                    raise ProvenanceExitExp
                if not self.IOStatsLsn_Proc.is_alive():
                    log(Mode.MAIN, "[Process Dead] The file_op_stats process has been terminated")
                    raise ProvenanceExitExp
                if not self.aggregator_Proc.is_alive():
                    log(Mode.MAIN, "[Process Dead] The Aggregator process has been terminated")
                    raise ProvenanceExitExp

        except ProvenanceExitExp:
            log(Mode.APP_EXIT, "***************** Provenance Server Stopped *****************")
            if not self.IOStatsLsn_Proc is None:
                self.IOStatsLsn_Proc.terminate()
                # self.IOStatsLsn_Proc.join()

            if not self.fileOPStats_Proc is None:
                self.fileOP_Q.close()
                self.fileOP_Q.join_thread()
                self.fileOPStats_Proc.terminate()
                self.fileOPStats_Proc.join()

            if not self.aggregator_Proc is None:
                # self.aggregator_Proc.terminate()
                self.aggregator_Proc.event_flag.set()
                self.aggregator_Proc.join()

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

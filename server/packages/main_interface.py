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
from file_op_logs import ChangeLogCollector
from communication import CommunicationExp
from config import ConfigReadExcetion
from exceptions import ProvenanceExitExp
from multiprocessing import Queue
from time import sleep, ctime
import signal, sys, os

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

        # make sure the $PYTHONHASHSEED is disabled otherwise the hash function for each object
        # generates different hash number at each session and will cause a huge mess in database
        # if os.getenv('PYTHONHASHSEED') != '0':
        #     print("[ERROR] _MAIN_INTERFACE_: the PYTHONHASHSEED environment variable must be set to '0': \n\n"
        #           "     export PYTHONHASHSEED=0\n")
        #     sys.exit(-1)

    # Handle the SIGINT and SIGTERM signals in order to shutdown the server
    def server_exit(self, sig, frame):
        raise ProvenanceExitExp

    # Main Function
    def run_server(self):
        try:

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
                #if not self.fileOPStats_Proc.is_alive() or not self.IOStatsLsn_Proc.is_alive() or not self.aggregator_Proc.is_alive():
                #    raise ProvenanceExitExp

        except ProvenanceExitExp:
            print("\nProvenance Server is shutting down...")

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

        except CommunicationExp as commExp:
            print(commExp.getMessage())

        except IOStatsException as iostExp:
            print(iostExp.getMessage())

        except AggregatorException as aggrExp:
            print(aggrExp.getMessage())

        except Exception as exp:
            print(str(exp))

        finally:

            if not self.IOStatsLsn_Proc is None:
                #self.MSDStat_Q.close()
                #self.MSDStat_Q.join_thread()
                #self.OSSStat_Q.close()
                #self.OSSStat_Q.join_thread()
                self.IOStatsLsn_Proc.terminate()
                #self.IOStatsLsn_Proc.join()

            if not self.fileOPStats_Proc is None:
                self.fileOP_Q.close()
                self.fileOP_Q.join_thread()
                self.fileOPStats_Proc.terminate()
                self.fileOPStats_Proc.join()

            if not self.aggregator_Proc is None:
                #self.aggregator_Proc.terminate()
                self.aggregator_Proc.event_flag.set()
                self.aggregator_Proc.join()

            print("Done!")


#
# Main
#
if __name__ == "__main__":
    mainInterface = Main_Interface()
    mainInterface.run_server()

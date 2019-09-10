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
from communication import CommunicationExp
from config import ConfigReadExcetion
from multiprocessing import Queue
from time import sleep, ctime
import signal

#
# The main class which is executed by the main Daemon process
#
class Main_Interface:
    def __init__(self):
        self.IOStatsLsn_Proc = None
        self.aggregator_Proc = None
        self.count = 0

        # Register signal handler
        signal.signal(signal.SIGINT, self.server_exit)
        signal.signal(signal.SIGTERM, self.server_exit)

    # Handle the SIGINT and SIGTERM signals in order to shutdown the server
    def server_exit(self, sig, frame):
        raise ProvenanceExitExp

    # Main Function
    def run_server(self):
        try:
            fsIOstat_Q = Queue()

            # IO Stats Listener Process
            self.IOStatsLsn_Proc = IOStatsListener(fsIOstat_Q)
            self.IOStatsLsn_Proc.start()

            # Aggregator Process
            self.aggregator_Proc = Aggregator(fsIOstat_Q)
            self.aggregator_Proc.start()

            while True:
                sleep(0.5)
                if not self.IOStatsLsn_Proc.is_alive() or not self.aggregator_Proc.is_alive():
                    raise ProvenanceExitExp

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
                self.IOStatsLsn_Proc.terminate()

            if not self.aggregator_Proc is None:
                self.aggregator_Proc.terminate()

            print("Done!")

#
#  Exception will be raised when SIGINT or SIGTERM are called
#
class ProvenanceExitExp(Exception):
    pass

#
# Main
#
if __name__ == "__main__":
    mainInterface = Main_Interface()
    mainInterface.run_server()

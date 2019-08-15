# -*- coding: utf-8 -*-
"""
    The main module runs on the Provenance Server machine. This module
    executes of several concurrent tasks:

        1. Receiveing I/O statistic data from Provenance agent on Lustre servers
        2. Collecting Lustre Changelog data from the client side
        3. Aggregate the data from 1 & 2 in a meaningful way
        4. Store data on Database

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from communication import CommunicationExp
from file_io_stats import IOStatsListener
from config import ConfigReadExcetion
from time import sleep, ctime
from queue import Queue
import signal
import sys

#
# The main class which is executed by the main Daemon process
#
class Main_Interface:
    def __init__(self):
        self.IOStatsLsn_Proc = None

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

            # IO Stats Listener Thread
            self.IOStatsLsn_Proc = IOStatsListener(fsIOstat_Q)
            self.IOStatsLsn_Proc.start()

            self.IOStatsLsn_Proc.join()

            while True:
                sleep(0.5)
                if not self.IOStatsLsn_Proc.is_alive():
                    raise ProvenanceExitExp

        except ProvenanceExitExp:
            print("\nProvenance Server is shutting down...")

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

        except CommunicationExp as commExp:
            print(commExp.getMessage())

        except Exception as exp:
            print(str(exp))

        finally:
            if not self.IOStatsLsn_Proc == None:
                self.IOStatsLsn_Proc.terminate()

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

# -*- coding: utf-8 -*-
"""
    The main module runs on the Provenance Server machine. This module
    executes of several concurrent tasks:

        1. Receiveing I/O statistic data from Provenance agent on Lustre servers
        2. Collecting Lustre Changelog data from the client side
        3. Aggregate the data from 1 & 2 in a meaningful way
        4. Store data on Database
"""

from Communication import ServerConnection, CommunicationExp
from Config import ServerConfig, ConfigReadExcetion
from multiprocessing import Process
from threading import Thread, Event
from time import sleep, ctime
from queue import Queue
import signal
import json
import sys
#
#
#
class IOStatsListener(Process):
    def __init__(self, fsIOstat_Q):
        Process.__init__(self)
        self.fsIOstat_Q = fsIOstat_Q
        self.config = ServerConfig()
        # Register signal handler
        signal.signal(signal.SIGINT, self.__exit)
        signal.signal(signal.SIGTERM, self.__exit)

    # Implement Thread.run()
    def run(self):
        try:
            # Create server connection
            comm = ServerConnection()
            # Start collecting IO statistics
            # ioStats_recv function will take care of incoming data
            self.__conn, self.__channel = comm.Collect_io_stats(self.ioStats_receiver)

        except CommunicationExp as commExp:
            print(commExp.getMessage())

        except ProvenanceExitExp:
            pass

        except Exception as exp:
            print(str(exp))

    # This function will be triggered as soon as RabbitMQ receives data from
    # agents on jobStat queue
    def ioStats_receiver(self, ch, method, properties, body):
        #parsed = json.loads(body)
        #data = json.dumps(parsed, indent=4, sort_keys=True)
        print(" [=>] Received %r\n" %body)

    # Handle the SIGINT and SIGTERM signals in order to shutdown the Process
    def __exit(self, sig, frame):
        raise ProvenanceExitExp


#
#  Exception will be raised when SIGINT or SIGTERM are called
#
class ProvenanceExitExp(Exception):
    pass

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
# Main
#
if __name__ == "__main__":
    mainInterface = Main_Interface()
    mainInterface.run_server()

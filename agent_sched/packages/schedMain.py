# -*- coding: utf-8 -*-
"""
    The main Module that keeps listening for any incoming RPC calls in order to
    run the appropreate method to respond to the RPC request.

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from schedConfig import SchedConfig, ConfigReadExcetion
from schedComm import SchedConnection, CommunicationExp
from ugeSchedService import UGEAccountingInfo
import signal
import json

class SchedMain:
    def __init__(self):
        self.__config = SchedConfig()
        self.__rpc_queue = self.__config.getRPC_queue()
        self.__rpc_proc = None

        signal.signal(signal.SIGINT, self.agent_exit)
        signal.signal(signal.SIGTERM, self.agent_exit)

    # Handle the SIGINT and SIGTERM signals in order to shutdown
    # the Collector agent
    def agent_exit(self, sig, frame):
        raise SchedExitExp

    # Start the Sched Service
    def run_sched_service(self):
        try:
            # Make Sure the rpc_queue has been defined
            if not self.__rpc_queue:
                raise ConfigReadExcetion("'rpc_queue' is not defined in 'sched.conf' file.")

            # Initialize RPC Communication
            schedComm = SchedConnection(is_rpc=True)
            # Start listening and replying to RPC requests
            schedComm.start_RPC_server(self.__on_rpc_callback)

        except SchedExitExp:
            print("\nProvenance SCHED agent is shutting down...")

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

        except CommunicationExp as commExp:
            print(commExp.getMessage())

        # except Exception as exp:
        #     print(str(exp))

        finally:
            if self.__rpc_proc and self.__rpc_proc.is_alive():
                self.__rpc_proc.terminate()

    #
    # Receiving RPC request and handling the response that has to be sent
    # back to the RPC client
    #
    def __on_rpc_callback(self, request: str) -> str:
        request = json.loads(request)
        if request['action'] == 'uge_acct':
            ugeAcctInfo = UGEAccountingInfo(self.__config)
            ugeAcctLst = ugeAcctInfo.getAcctountingInfo(request['data'])
            if ugeAcctLst:
                return '[^@]'.join(ugeAcctLst)
            return 'NONE'

        # else return nothing
        return 'NONE'
#
#  Exception will be raised when SIGINT or SIGTERM are called
#
class SchedExitExp(Exception):
    pass


if __name__ == "__main__":
    schedMain = SchedMain()
    schedMain.run_sched_service()

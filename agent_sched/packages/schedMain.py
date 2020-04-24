# -*- coding: utf-8 -*-
"""
    The main Module that keeps listening for any incoming RPC calls in order to
    run the appropreate method to respond to the RPC request.

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from subprocess import run, CalledProcessError, DEVNULL
from schedConfig import SchedConfig, ConfigReadExcetion
from schedComm import SchedConnection, CommunicationExp
from ugeSchedService import UGEAccountingInfo
from schedLogger import log, Mode
import signal
import json
import sys

class SchedMain:
    def __init__(self):
        try:
            self._config = SchedConfig()
            self._rpc_queue = self._config.getRPC_queue()
            self._rpc_vhost = self._config.getVhost()
            self._rpc_proc = None

            signal.signal(signal.SIGINT, self.agent_exit)
            signal.signal(signal.SIGTERM, self.agent_exit)

            self._cleanup_RPC_que()

        except ConfigReadExcetion as confExp:
            log(Mode.MAIN_SCHED, confExp.getMessage())

    # Handle the SIGINT and SIGTERM signals in order to shutdown
    # the Collector agent
    def agent_exit(self, sig, frame):
        raise SchedExitExp

    # Start the Sched Service
    def run_sched_service(self):
        try:
            log(Mode.APP_START, "***************** Provenance Sched Service Started *****************")
            # Make Sure the rpc_queue has been defined
            if not self._rpc_queue:
                raise ConfigReadExcetion("'rpc_queue' is not defined in 'sched.conf' file.")

            # Initialize RPC Communication
            schedComm = SchedConnection(is_rpc=True)
            # Start listening and replying to RPC requests
            schedComm.start_RPC_server(self._on_rpc_callback)

        except SchedExitExp:
            log(Mode.APP_EXIT, "***************** Provenance Sched Service Stopped *****************")
            if self._rpc_proc and self._rpc_proc.is_alive():
                self._rpc_proc.terminate()

        except ConfigReadExcetion as confExp:
            log(Mode.MAIN_SCHED, confExp.getMessage())

        except CommunicationExp as commExp:
            log(Mode.MAIN_SCHED, commExp.getMessage())

        except Exception as exp:
            log(Mode.MAIN_SCHED, str(exp))


    #
    # Receiving RPC request and handling the response that has to be sent
    # back to the RPC client
    #
    def _on_rpc_callback(self, request: str) -> str:
        request = json.loads(request)
        if request['action'] == 'uge_acct':
            ugeAcctInfo = UGEAccountingInfo(self._config)
            ugeAcctLst = ugeAcctInfo.getAcctountingInfo(request['data'])
            if ugeAcctLst:
                return '[^@]'.join(ugeAcctLst)
            return 'NONE'

        # else return nothing
        return 'NONE'

    #
    # Cleanup the RabbitMQ RPC Queue
    #
    def _cleanup_RPC_que(self):
        try:
            run(f"rabbitmqctl -p {self._rpc_vhost} purge_queue {self._rpc_queue}",
                shell=True, check=True, stdout=DEVNULL)
        except CalledProcessError as runExp:
            log(Mode.MAIN_SCHED, str(runExp))
            sys.exit(-1)

#
#  Exception will be raised when SIGINT or SIGTERM are called
#
class SchedExitExp(Exception):
    pass


if __name__ == "__main__":
    schedMain = SchedMain()
    schedMain.run_sched_service()

# -*- coding: utf-8 -*-
"""
    This module helps to store some of the temporary information in a file just for a while:
        - Finished Jobs:
          Once a job gets finished we no more want to performa any action such as :I/O & OP collection,
          aggregation, retrieve JobInfo and store into database. However, there is no mechanist to remove
          a particular JobStat data from Lustre Servers. On the other hand, keeping finished JobIds in
          memory is not a reliable method since restarting the Provenance_server will lose the rack of
          finished jobs while Lustre still maitians the JobStat data of finished jobs until MAX_CLEAN_UP
          comes up.
"""
from config import ServerConfig, ConfigReadExcetion
from logger import log, Mode
from pathlib import Path
from typing import Dict
import fcntl
import json

class FinishedJobs:
    def __init__(self):
        # Get the absolute path of finished job id file
        self._fJobs_path = Path(__file__).parent.joinpath("finjobids.dat").absolute()
        # Create it if it does not exist
        Path(self._fJobs_path).touch(exist_ok=True)
        # Get String posix format
        self._fJobs_file = self._fJobs_path.as_posix()

    # Initialize the finished Jobs DB based on OSSs and MDSs
    def init(self):
        try:
            config  = ServerConfig()
            # get the list of all OSSs and MDSs
            lustre_servers = config.getOSS_hosts() + config.getMDS_hosts()
            # Define a Dictionary to holds: {server1 : [jobid1, jobid2, ...], server2: [jobid1,...], ...}
            finJobsTable = {}
            # Get the file content if it's not empty
            if self._fJobs_path.stat().st_size > 0:
                with open(self._fJobs_file, 'r') as fjobFile:
                    finJobsTable = json.load(fjobFile)
            # Create/Update the finished jobs table on file
            for server in lustre_servers:
                if server not in finJobsTable.keys():
                    finJobsTable[server] = []
            # dump back the finished jobs table to the file
            with open(self._fJobs_file, 'w') as fjobFile:
                json.dump(finJobsTable, fjobFile)

        except ConfigReadExcetion as confExp:
            log(Mode.PERSISTENT, confExp.getMessage())

        except Exception as exp:
            log(Mode.PERSISTENT, str(exp))

    # Store the JobId of an already finished job
    def add(self, fJobId):
        fjobFile = open(self._fJobs_file, "r+")
        # lock the file to prevent race condition between processes
        fcntl.flock(fjobFile, fcntl.LOCK_EX)
        try:
            # Get the current database
            finJobsTable = json.load(fjobFile)
            # add the fJobId for all servers
            for server in finJobsTable.keys():
                if fJobId not in finJobsTable[server]:
                    finJobsTable[server].append(fJobId)
            # Update the Finished Jobs File
            fjobFile.seek(0)
            json.dump(finJobsTable, fjobFile)
            fjobFile.truncate()
        finally:
            fcntl.flock(fjobFile, fcntl.LOCK_UN)
            fjobFile.close()

    # Get all Finished job IDs
    def getAll(self) -> Dict:
        with open(self._fJobs_file, "r") as fjobFile:
            finJobsTable = json.load(fjobFile)

        #print(f"GetAll: {finJobsTable}")
        return finJobsTable

    # Given a list of JobIds that are no more relevant since they are
    # already finished and no JobStat exist for them on Lustre
    def refine(self, server_name: str, invalidIdList: list):
        fjobFile = open(self._fJobs_file, "r+")
        # lock the file to prevent race condition between processes
        fcntl.flock(fjobFile, fcntl.LOCK_EX)
        try:
            # Get the current database
            finJobsTable = json.load(fjobFile)
            # Get the list of Finished Jobs of the server
            finJobsLst: list = finJobsTable[server_name]
            # Refine the list (Remove invalid JobIds)
            for jobId in finJobsLst:
                if jobId in invalidIdList:
                    finJobsLst.remove(jobId)
            # Assign the refined list to the server
            finJobsTable[server_name] = finJobsLst
            #print(f"refine: {finJobsTable}")
            # Update the Finished Jobs File
            fjobFile.seek(0)
            json.dump(finJobsTable, fjobFile)
            fjobFile.truncate()
        finally:
            fcntl.flock(fjobFile, fcntl.LOCK_UN)
            fjobFile.close()

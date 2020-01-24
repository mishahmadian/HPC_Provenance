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
from pathlib import Path

class FinishedJobs:
    def __init__(self):
        self.fJobs_file = "./fjobids.dat"
        Path(self.fJobs_file).touch(exist_ok=True)

    # Store the JobId of an already finished job
    def store(self, fJobId):
        with open(self.fJobs_file, "a") as fjobFile:
            fjobFile.write(str(fJobId) + "\n")

    # Get all Finished job IDs
    def getAll(self) -> list:
        jobIdLst = []
        with open(self.fJobs_file, "r") as fjobFile:
            for jobId in fjobFile.readlines():
                jobIdLst.append(jobId.strip())

        return jobIdLst

    # Given a list of JobIds that are no more relevant since they are
    # already finished and no JobStat exist for them on Lustre
    def correct_list(self, rJobIDLst: list):
        if rJobIDLst:
            with open(self.fJobs_file, "r+") as fjobFile:
                fJobIds = fjobFile.readlines()
                fjobFile.seek(0)
                for jobId in fJobIds:
                    if jobId.strip() not in rJobIDLst:
                        fjobFile.write(jobId)
                fjobFile.truncate()
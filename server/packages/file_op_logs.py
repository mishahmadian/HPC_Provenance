# -*- coding: utf-8 -*-
"""
    This module colects the Lustre ChangeLog data for all users file operation activities
    from an arbitrary client node (preferebly the Provenance Server node)

        1. Collecting Changelog data from a Lustre client
            - We assume the Provenance server has already mounted a lustre client
        2. Mapping the collected data into the corresponding object and place it into a queue for
            data aggregation with file IO stats.

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from multiprocessing import Process, Event, Queue, Pool, cpu_count
from config import ServerConfig, ConfigReadExcetion
from datetime import datetime
from typing import List
from pprint import pprint
from math import ceil
import subprocess
import time
import os
#
# This Class defines a new process to collect Lustre changelog data from
# Lustre client. The changelog has to be registered on MGS server.
#
class ChangeLogCollector(Process):
    def __init__(self, fileOP_Q: Queue):
        Process.__init__(self)
        self.fileOP_Q = fileOP_Q
        self.event_flag = Event()
        self.config = ServerConfig()
        try:
            self.__mdtTargets = self.config.getMdtTargets()
            self.__interval = self.config.getChLogsIntv()
            self.__chLogUsers = self.config.getChLogsUsers()
            __procNum = self.config.getChLogsPocnum()
            # If number of processes are less than 1 then all the available processes will be used
            self.__procNum = __procNum if __procNum > 0 else cpu_count()

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

    # Implement Process.run()
    def run(self):
        # keep the last record that has been processed and use that as the first record
        # that should be processed for each MDT Target. The default value of zero let
        # ChangLogs to be captured from the beginning
        lastCapturedRec = [0 for rec in range(0, len(self.__mdtTargets))]

        while not self.event_flag.is_set():
            # Create a list of FileOpObj objects
            fileOpObj_Lst: List[FileOpObj] = []
            print("I'm here")
            startT = time.time()
            for inx, mdtTarget in enumerate(self.__mdtTargets):
                # Collect ChangeLogs from Client Lustre filesystem
                print(lastCapturedRec[inx])
                chLogs_out = self.__collectChangeLogs(mdtTarget, lastCapturedRec[inx])
                print("I'm here too")
                # convert each row into an item in a List for parallel processing the list
                chLogOutputs: List[str] = chLogs_out.splitlines()
                # If the list of chLogOutputs is empty, then ignore following lines
                print(len(chLogOutputs))
                if not (len(chLogOutputs)):
                    print("No more record")
                    continue

                # Define the size of each chunk of data than needs to be processed by each process
                if self.__procNum > 1:
                    chunkSize = ceil(len(chLogOutputs) / 2 / self.__procNum)
                else:
                    chunkSize = 1

                # Create a pool of process, and assign the number of process which is defined by user
                pool = Pool(processes = self.__procNum if self.__procNum > 0 else cpu_count())
                # run a poll of process to map the ChangeLogs outputs line by line to FileOpObj objects
                # Those lines that do not have JobId will be ignored at this time!
                results = pool.starmap(self.changeLogs2FileOpsObj,
                                          [(chlog, mdtTarget) for chlog in chLogOutputs if "j=" in chlog],
                                          chunksize=chunkSize)
                pool.close()
                pool.join()
                # Place the results from all the processes into a List
                for fileOpObj in results:
                    # add the fileOpObj into fileOpObj_lst
                    fileOpObj_Lst.append(fileOpObj)

                # Put the list of File Operations info in the main Queue which is shared with Aggregator
                self.fileOP_Q.put(fileOpObj_Lst)

                # corresponding user to this MDT Target:
                user = self.__chLogUsers[inx]
                # last rec# defines the last record than should be cleared up to.
                # The last object in the list holds the last Rec#
                endRecNum = fileOpObj_Lst[-1].recID
                # Keep the endRecNum in array cell corresponding to the MDT Targets index
                lastCapturedRec[inx] = endRecNum
                # Clear off the ChangeLogs (optimization)
                ###self.__clearChangeLogs(mdtTarget, user, endRecId)

                pprint(vars(fileOpObj_Lst[-1]))
                print("Last Rec#: " + str(fileOpObj_Lst[-1].recID))

            print("I'm done")
            print("proc#={}  Time: {}".format(self.__procNum, (time.time() - startT)))
            # wait between collecting ChangeLogs
            self.event_flag.wait(self.__interval)

    # Collecting the lustre ChangeLogs data
    def __collectChangeLogs(self, mdtTarget: str, startRec: int) -> str:
        print("mdtTarget: {}    startRec: {}".format(mdtTarget, startRec))
        results = subprocess.check_output("lfs changelog " + mdtTarget + " " + str(startRec + 1), shell=True).decode("utf-8")
        print("I got in here")
        return results

    # Clear old ChangeLogs records
    def __clearChangeLogs(self, mdtTarget: str, user: str, endRec: int):
        subprocess.check_output("lfs changelog_clear " + mdtTarget + " " + user + " " + str(endRec), shell=True)

    # Convert (Parse & Compile) the ChangeLog output to FileOpObj object
    @staticmethod
    def changeLogs2FileOpsObj(chLogOutput: str, mdtTarget: str):
        # create a new FileOpObj object per record
        fileOpObj = FileOpObj()
        # records splits by space
        records = chLogOutput.split(' ')
        # Fill the fileOpObj Object:
        fileOpObj.setMdtTarget(mdtTarget)  # pass the mdtTarget that changeLogs where collected from
        fileOpObj.setRecID(records[0])  # Record Id
        fileOpObj.setOpType(records[1])  # The Type of File Operation
        fileOpObj.setDateTimeStamp(records[2], records[3])  # Date&TimeStamp based on Time and Date of each record
        # -- skip record[4] which is an operation type flag
        fileOpObj.setTargetFid(records[5], mdtTarget)  # Target FID
        fileOpObj.setJobInfo(records[6])
        #
        # The following records may or may not show up based on the operation type or host
        if len(records) > 7:
            # -- skip record[7] which is an extended flag
            for inx, rec in enumerate(records[7:]):

                if "u=" in rec:
                    fileOpObj.setUserInfo(rec)  # the UID and GID of the target if provided

                elif "nid=" in rec:
                    fileOpObj.setNid(rec)  # NID (IP@<lnet>) of the target host if Provided

                elif "p=" in rec:
                    fileOpObj.setParentFid(rec, mdtTarget)  # Parent FID of the target if provided

                elif (inx == len(records[7:]) - 1) and ("=" not in rec):
                    fileOpObj.setTargetFile(rec)  # Anything else should be the target file name if applicable

        return fileOpObj

#
# Object class that holds the parsed data from Lustre  ChangeLogs.
# this class will be imported in other packages/classes
#
class FileOpObj:
    def __init__(self):
        self.recID = 0
        self.jobid = None
        self.cluster = None
        self.sched_type = None
        self.procid = None
        self.op_type = None
        self.datetimestamp = None
        self.target_fid = None
        self.target_path = None
        self.uid = None
        self.gid = None
        self.nid = None
        self.parent_fid = None
        self.parent_path = None
        self.target_file = None
        self.mdtTarget = None

    def setRecID(self, recID):
        self.recID = recID

    def setJobInfo(self, jobInfo):
        jobInfo = jobInfo.split('=')[1].strip()
        # it can be a executable Job or a Process
        if '_' in jobInfo: # this type of JobInfo comes from scheduler
            self.cluster, self.sched_type, self.jobid = \
                    jobInfo.strip().split('_')
        elif jobInfo:
            self.procid = jobInfo

    def setOpType(self, opinfo):
        # Ignore the Operation code
        self.op_type = ''.join([ch for ch in opinfo if not ch.isdigit()])

    def setDateTimeStamp(self, time_str, date_str):
        # (Naive way) Covert the Nanosecond to Millisecond since Python does not support Nanoseconds
        datetime_str = time_str[:-3] + " " + date_str
        date_time_obj = datetime.strptime(datetime_str, '%H:%M:%S.%f %Y.%m.%d')
        self.datetimestamp = datetime.timestamp(date_time_obj)

    def setTargetFid(self, tfid, mdtTarget):
        self.target_fid = tfid.split('=')[1].strip()
        try:
            with open(os.devnull, 'w') as nullDev:
                self.target_path = subprocess.check_output("lfs fid2path " + mdtTarget + " " + self.target_fid,
                                                           shell=True, stderr=nullDev).decode("utf-8").strip()
        except subprocess.CalledProcessError:
            # The file has been removed already and the fid is invalid
            self.target_path = "-1"

    def setUserInfo(self, userinfo):
        self.uid, self.gid = userinfo.strip().split('=')[1].split(':')

    def setNid(self, nid):
        self.nid = nid.split('=')[1].strip()

    def setParentFid(self, pfid, mdtTarget):
        self.parent_fid = pfid.split('=')[1].strip()
        try:
            with open(os.devnull, 'w') as nullDev:
                self.parent_path = subprocess.check_output("lfs fid2path " + mdtTarget + " " + self.parent_fid,
                                                           shell=True, stderr=nullDev).decode("utf-8").strip()
        except subprocess.CalledProcessError:
            # The file has been removed already and the fid is invalid
            self.parent_path = "-1"

    def setTargetFile(self, name):
        self.target_file = name.strip()

    def setMdtTarget(self, mdtTarget):
        self.mdtTarget = mdtTarget

    # Overriding the Hash function for this object
    def __hash__(self):
        if self.procid:
            # No hash for this object if jobID is not defined
            return None
        return hash((self.jobid, self.cluster, self.sched_type))

        

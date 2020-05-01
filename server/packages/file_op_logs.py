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
from subprocess import check_output, CalledProcessError, DEVNULL
from config import ServerConfig, ConfigReadExcetion
from exceptions import ProvenanceExitExp
from datetime import datetime
from logger import log, Mode
from typing import List
from math import ceil
import hashlib
import ctypes
#
# Lustre Change Logs Process
#
class ChangeLogCollector(Process):
    """
    This Class defines a new process to collect Lustre changelog data from
    Lustre client. The changelog has to be registered on MGS server.
    """
    def __init__(self, fileOP_Q: Queue, shut_down: Event):
        Process.__init__(self)
        self.fileOP_Q = fileOP_Q
        self.event_flag = Event()
        self._shutdown = shut_down
        self.config = ServerConfig()
        try:
            self._mdtTargets = self.config.getMdtTargets()
            self._interval = self.config.getChLogsIntv()
            self._chLogUsers = self.config.getChLogsUsers()
            _procNum = self.config.getChLogsPocnum()
            # If number of processes are less than 1 then all the available processes will be used
            self._procNum = _procNum if _procNum > 0 else cpu_count()

        except ConfigReadExcetion as confExp:
            log(Mode.FILE_OP_LOGS, confExp.getMessage())

    # Implement Process.run()
    def run(self):
        # keep the last record that has been processed and use that as the first record
        # that should be processed for each MDT Target. The default value of zero let
        # ChangLogs to be captured from the beginning
        lastCapturedRec = [0 for _ in range(0, len(self._mdtTargets))]
        # Pool of processes that parse the ChangeLogs
        pool = None
        try:
            while not (self.event_flag.is_set() or self._shutdown.is_set()):

                for inx, mdtTarget in enumerate(self._mdtTargets):
                    # Create a list of FileOpObj objects
                    fileOpObj_Lst: List[FileOpObj]
                    # Collect ChangeLogs from Client Lustre filesystem
                    chLogs_out = self._collectChangeLogs(mdtTarget, lastCapturedRec[inx])
                    # convert each row into an item in a List for parallel processing the list
                    chLogOutputs: List[str] = chLogs_out.splitlines()
                    # If the list of chLogOutputs is empty, then ignore following lines
                    if not (len(chLogOutputs)):
                        continue

                    # Define the size of each chunk of data than needs to be processed by each process
                    if self._procNum > 1:
                        chunkSize = ceil(len(chLogOutputs) / 2 / self._procNum)
                    else:
                        chunkSize = 1

                    # Create a pool of process, and assign the number of process which is defined by user
                    pool = Pool(processes = self._procNum if self._procNum > 0 else cpu_count())
                    # run a poll of process to map the ChangeLogs outputs line by line to FileOpObj objects
                    # Those lines that do not have JobId will be ignored at this time!
                    fileOpObj_Lst = pool.starmap(self.changeLogs2FileOpsObj,
                                              [(chlog, mdtTarget) for chlog in chLogOutputs if "j=" in chlog],
                                              chunksize=chunkSize)
                    pool.close()
                    pool.join()

                    # There is a chance that fileOpObj_Lst might be empty if all the records have no "j="
                    if fileOpObj_Lst:
                        # Put the list of File Operations info in the main Queue which is shared with Aggregator
                        self.fileOP_Q.put(fileOpObj_Lst)

                        # corresponding user to this MDT Target:
                        user = self._chLogUsers[inx]
                        # last rec# defines the last record than should be cleared up to.
                        # The last object in the list holds the last Rec#
                        endRecNum = fileOpObj_Lst[-1].recID
                        # Keep the endRecNum in array cell corresponding to the MDT Targets index
                        lastCapturedRec[inx] = int(endRecNum)
                        # Clear off the ChangeLogs (optimization)
                        ###self._clearChangeLogs(mdtTarget, user, endRecId)

                # wait between collecting ChangeLogs
                self.event_flag.wait(self._interval)

        except ProvenanceExitExp:
            pass

        finally:
            if pool:
                pool.terminate()
                pool.join()


    # Collecting the lustre ChangeLogs data
    def _collectChangeLogs(self, mdtTarget: str, startRec: int) -> str:
        """
        Utilizing "lfs" command of the local machine and captures the File Operations from Lustre client

        :param mdtTarget: String - MDT Targets should already be defined in server.conf configuration file
        :param startRec: Integer -  Captures from where it left off
        :return: Line-separated records
        """
        return check_output("lfs changelog " + mdtTarget + " " + str(startRec + 1), shell=True).decode("utf-8")

    # Clear old ChangeLogs records
    def _clearChangeLogs(self, mdtTarget: str, user: str, endRec: int):
        check_output("lfs changelog_clear " + mdtTarget + " " + user + " " + str(endRec), shell=True)

    # Convert (Parse & Compile) the ChangeLog output to FileOpObj object
    @staticmethod
    def changeLogs2FileOpsObj(chLogOutput: str, mdtTarget: str):
        try:
            # create a new FileOpObj object per record
            fileOpObj = FileOpObj()
            # records splits by space
            records = chLogOutput.split(' ')
            # Fill the fileOpObj Object:
            fileOpObj.setMdtTarget(mdtTarget)  # pass the mdtTarget that changeLogs where collected from
            fileOpObj.setRecID(records[0])  # Record Id
            fileOpObj.setOpType(records[1])  # The Type of File Operation
            fileOpObj.setTimestamp(records[2], records[3])  # Date&TimeStamp based on Time and Date of each record
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

                    elif "m=" in rec:
                        fileOpObj.setOpenMode(rec)

                    elif "x=" in rec:
                        fileOpObj.setXattr(rec)

                    elif (inx == len(records[7:]) - 1) and ("=" not in rec):
                        fileOpObj.setTargetFile(rec)  # Anything else should be the target file name if applicable

            return fileOpObj

        except ProvenanceExitExp:
            pass

#
# Object class that holds the parsed data from Lustre  ChangeLogs.
# this class will be imported in other packages/classes
#
class FileOpObj(object):
    def __init__(self):
        self.recID = 0
        self.jobid = None
        self.taskid = None
        self.cluster = None
        self.sched_type = None
        self.procid = None
        self.op_type = None
        self.open_mode = None
        self.ext_attr = None
        self.timestamp = None
        self.target_fid = None
        self.target_path = None
        self.userid = None
        self.groupid = None
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
            # if the jobid is separated by '.' then it means the job is an array job
            if '.' in self.jobid:
                self.jobid, self.taskid = self.jobid.split('.')

        elif jobInfo:
            self.procid = jobInfo

    def setOpType(self, opinfo):
        # Ignore the Operation code
        self.op_type = ''.join([ch for ch in opinfo if not ch.isdigit()])

    def setOpenMode(self, mode):
        self.open_mode = mode.split('=')[1].strip()

    def setXattr(self, xattr):
        self.ext_attr = xattr.split('=')[1].strip()

    def setTimestamp(self, time_str, date_str):
        # (Naive way) Covert the Nanosecond to Millisecond since Python does not support Nanoseconds
        datetime_str = time_str[:-3] + " " + date_str
        date_time_obj = datetime.strptime(datetime_str, '%H:%M:%S.%f %Y.%m.%d')
        self.timestamp = datetime.timestamp(date_time_obj)

    def setTargetFid(self, tfid, mdtTarget):
        self.target_fid = tfid.split('=')[1].strip()
        try:
            self.target_path = check_output("lfs fid2path --link 1 " + mdtTarget + " " + self.target_fid,
                                                       shell=True, stderr=DEVNULL).decode("utf-8").strip()
        except CalledProcessError:
            # The file has been removed already and the fid is invalid
            self.target_path = "File Not Exist"

    def setUserInfo(self, userinfo):
        self.userid, self.groupid = userinfo.strip().split('=')[1].split(':')

    def setNid(self, nid):
        self.nid = nid.split('=')[1].strip()

    def setParentFid(self, pfid, mdtTarget):
        self.parent_fid = pfid.split('=')[1].strip()
        try:
            self.parent_path = check_output("lfs fid2path --link 1 " + mdtTarget + " " + self.parent_fid,
                                                       shell=True, stderr=DEVNULL).decode("utf-8").strip()
        except CalledProcessError:
            # The file has been removed already and the fid is invalid
            self.parent_path = "File Not Exist"

    def setTargetFile(self, name):
        self.target_file = name.strip()

    def setMdtTarget(self, mdtTarget):
        self.mdtTarget = mdtTarget

    # This function returns a unique ID for every objects with the same JobID, Scheduler, and cluster
    def uniqID(self):
        if self.procid:
            # No hash for this object if jobID is not defined
            return None
        # calculate the MD5 hash
        obj_id = ''.join(filter(None, [self.sched_type, self.cluster, self.jobid, self.taskid]))
        hash_id = hashlib.md5(obj_id.encode(encoding='UTF=8'))
        return hash_id.hexdigest()

    # Return a dictionary format of all attrs and their values
    def to_dict(self) -> dict:
        attrDict = {}
        # collect all available attributes
        attrs = [atr for atr in dir(self) if (not atr.startswith('__')) and (not callable(getattr(self, atr)))]
        for attr in attrs:
            attrDict[attr] = getattr(self, attr)
        # append the unique ID
        attrDict['uid'] = self.uniqID()
        #
        return attrDict

    # Overriding the Hash function for this object
    def __hash__(self):
        if self.procid:
            # No hash for this object if jobID is not defined
            return None
        # calculate the hash
        hashVal = hash((self.jobid, self.cluster, self.sched_type))
        # make sure the value is always positive (we don't want negative hash to be used as ID)
        return ctypes.c_size_t(hashVal).value

        

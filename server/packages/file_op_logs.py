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
from config import ServerConfig, ConfigReadExcetion
from multiprocessing import Process, Event
from datetime import datetime
import subprocess
#
# This Class defines a new process to collect Lustre changelog data from
# Lustre client. The changelog has to be registered on MGS server.
#
class ChangeLogCollector(Process):
    def __init__(self, fileOP_Q):
        Process.__init__(self)
        self.fileOP_Q = fileOP_Q
        self.event_flag = Event()
        self.config = ServerConfig()
        try:
            self.__mdtTargets = self.config.getMdtTargets()
            self.__interval = self.config.getChLogsIntv()

        except ConfigReadExcetion as confExp:
            print(confExp.getMessage())

    # Implement Process.run()
    def run(self):
        while not self.event_flag.is_set():
            for mdtTarget in self.__mdtTargets:
                chLogs_out = self.__collectChangeLogs(mdtTarget)
                fileOpObj_Lst = self.__parseChaneLogs(chLogs_out, mdtTarget)

            # wait between collecting ChangeLogs
            self.event_flag.wait(self.__interval)

    # Collecting the lustre ChangeLogs data
    def __collectChangeLogs(self, mdtTarget):
        return subprocess.check_output("lfs changelog " + mdtTarget, shell=True)

    # Parse the ChangeLogs output line by line and place them into an array of changeLogs
    def __parseChaneLogs(self, chLogs, mdtTarget):
        # Create a list of FileOpObj objects
        fileOpObj_Lst = []
        # Iterate over changeLogs line by line
        for line in chLogs.splitlines():
            # create a new FileOpObj object per record
            fileOpObj = FileOpObj()
            # records splits by space
            records = line.split(' ')
            # Fill the fileOpObj Object:
            fileOpObj.setMdtTarget(mdtTarget) # pass the mdtTarget that changeLogs where collected from
            fileOpObj.setRecID(records[0]) # Record Id
            fileOpObj.setOpType(records[1]) # The Type of File Operation
            fileOpObj.setDateTimeStamp(records[2], records[3]) # Date&TimeStamp based on Time and Date of each record
            # -- skip record[4] which is an operation type flag
            fileOpObj.setTargetFid(records[5]) # Target FID
            fileOpObj.setJobInfo(records[6])
            #
            # The following records may or may not show up based on the operation type or host
            if len(records) > 7:
                # -- skip record[7] which is an extended flag
                for rec in records[7:]:
                    if "u=" in rec:
                        fileOpObj.setUserInfo(rec) # the UID and GID of the target if provided

                    elif "nid=" in rec:
                        fileOpObj.setNid(rec) # NID (IP@<lnet>) of the target host if Provided

                    elif "p=" in rec:
                        fileOpObj.setParentFid(rec) # Parent FID of the target if provided

                    else:
                        fileOpObj.setTargetFile(rec) # Anything else should be the target file name if applicable

            # add the fileOpObj into fileOpObj_lst
            fileOpObj_Lst.append(fileOpObj)

        return fileOpObj_Lst


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
        self.uid = None
        self.gid = None
        self.nid = None
        self.parent_fid = None
        self.target_file = None
        self.mdtTarget = None

    def setRecID(self, id):
        self.recID = id

    def setJobInfo(self, jobInfo):
        # it can be a executable Job or a Process
        if '_' in jobInfo:
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

    def setTargetFid(self, tfid):
        self.target_fid = tfid.split('=')[1].strip()

    def setUserInfo(self, userinfo):
        self.uid, self.gid = userinfo.strip().split(':')

    def setNid(self, nid):
        self.nid = nid

    def setParentFid(self, pfid):
        self.parent_fid = pfid.split('=')[1].strip()

    def setTargetFile(self, name):
        self.target_file = name

    def setMdtTarget(self, mdtTarget):
        self.mdtTarget = mdtTarget

        

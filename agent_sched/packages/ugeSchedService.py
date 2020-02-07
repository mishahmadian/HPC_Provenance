# -*- coding: utf-8 -*-
"""
    The schedulerService runs on the scheduler head node, where the job scheuler master process is runnign
    then it collects the required data from the job scheduler and publish them on the RabbitMQ Queue

        - By now it only works with Univa Grid Engine (UGE)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from schedConfig import SchedConfig, ConfigReadExcetion
from file_read_backwards import FileReadBackwards
from queue import Queue

class UGEAccountingInfo:
    def __init__(self, config : SchedConfig):
        self.__accountingFilePath = config.getUGE_acct_file()
        self.__maxReadLine = config.getMaxReadLine()

    def getAcctountingInfo(self, jobIdLst : list) -> list:
        acctList = []
        #The path to Accounting file should already be specified
        if not self.__accountingFilePath:
            raise ConfigReadExcetion("'accounting_file' is not defined in 'sched.conf' file")

        # Open the UGE Accounting file and start read it backward since latest job infos
        # could be found at the bottom of the file
        with FileReadBackwards(self.__accountingFilePath, encoding="utf-8") as ugeAcctFile: #ISO-8859-1
            # keep the rack of # of records that has to be read
            # in order to keep it efficient
            rec_counter = self.__maxReadLine

            while jobIdLst and rec_counter:
                try:
                    acctRec = ugeAcctFile.readline()
                    # Write off the max count
                    rec_counter -= 1
                    # Skip comments
                    if not acctRec.strip() or acctRec.startswith('#'):
                        continue
                    # Get the Job_number and Task_number for Accounting file
                    jobRec = acctRec.split(':')
                    jobid = str(jobRec[5])
                    taskid = str(jobRec[35])
                    job_task_id = '.'.join([jobid, taskid])

                    # find the requested rec
                    if job_task_id in jobIdLst:
                        acctList.append(acctRec.strip())
                        jobIdLst.remove(job_task_id)
                    # take off one round
                except UnicodeDecodeError:
                    # ignore the lines that come with non-Ascii characters
                    continue

        return acctList





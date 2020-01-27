# -*- coding: utf-8 -*-
"""
    This module handles all the communication with Univa Grid engine (UGE) Job Scheduler.
    In this module the required data corresponding to job will be recieved from:
        - UGE RESTFull API from the UGE Qmaster node
        - Accounting Data from Unisight MongoDB

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig
#from scheduler import UGEJobInfo
import json, urllib.request
import scheduler
import sys
#
# This class communicates with UGERestful API in order to collect required information
class UGERest:

    @staticmethod
    def getUGEJobInfo(ip_addr, port, jobId, taskId) -> 'scheduler.UGEJobInfo' or None:
        # Define the UGERest base URL
        ugerest_base_url = "http://" + ip_addr + ":" + port
        # UGERest JobInfo API URL
        ugerest_job_url = ugerest_base_url + "/jobs/" + str(jobId) + ("." + str(taskId) if taskId else ".1")
        # Get JobInfo from UGERest
        response = urllib.request.urlopen(ugerest_job_url)

        if response:
            # Read the JobInfo Data
            data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
            # If "errorCode 299" shows up it means the JobInfo is not available due to:
            #   1. Delay between the time that jobs starts running and the UGERest
            #   2. The job is already finished
            if 'errorCode' in data and data['errorCode'] == 299:
                return None

            else:
                # Convert JobInfo Json data into UGEJobInfo object
                jobInfo = scheduler.UGEJobInfo()
                jobInfo.jobid = jobId
                jobInfo.taskid = taskId
                jobInfo.status = scheduler.UGEJobInfo.Status(
                    {'r' : 1, 'qw' : 2, 'd' : 3, 'dr' : 3, 'E' : 4, 'Eqw' : 4}.get(data['state'], 5)
                )
                jobInfo.jobName = data['name']
                jobInfo.queue = data['queue'].split('@')[0]
                jobInfo.num_cpu = data['slots']
                jobInfo.submit_time = data['timeStamp']['submitEpoch']
                jobInfo.start_time = data['timeStamp']['startEpoch']
                jobInfo.username = data['user']
                # the Hard resources may or may not appear in UGERest json
                try:
                    for item in data['resources']['hard']:
                        if item['name'] == 'h_rt':
                            jobInfo.h_rt = item['value']

                        elif item['name'] == 's_rt':
                            jobInfo.s_rt = item['value']

                        elif item['name'] == 'h_vmem':
                            jobInfo.h_vmem = item['value']
                except KeyError:
                    pass

                jobInfo.parallelEnv = data['parallelEnv']
                jobInfo.project = data['project']
                jobInfo.command = data['command']
                # Read Environment Variables of the job
                try:
                    for env in data['jobEnvironment']:
                        if env['name'] == 'PWD':
                            jobInfo.pwd = env['value']

                except KeyError:
                    pass
                # Read Usage statistics of the job
                for attr in ['cpu', 'io', 'ioops', 'iow', 'maxvmem', 'mem', 'wallclock']:
                    try:
                        setattr(jobInfo, attr, data['usage'][attr])
                    except KeyError:
                        pass

                # Return generated JobInfo Object
                return jobInfo
#
# This Class communicates with UGE q_master node and tries to collect Accounting data when job is already finished
#
class UGEAccounting:

    @staticmethod
    def getUGEJobInfo(ip_addr, port, jobId, taskId) -> 'scheduler.UGEJobInfo' or None:
        pass

#
# In any case of Error, Exception, or Mistake UGEServiceException will be raised
#
class UGEServiceException(Exception):
    def __init__(self, message):
        super(UGEServiceException, self).__init__(message)
        self.message = "\n [Error] _UGE_SERVICE_: " + message + "\n"

    def getMessage(self):
        return self.message

if __name__ == "__main__":
    config = ServerConfig()
    cluster_inx = 0
    uge_ip = config.getUGE_Addr()[cluster_inx]
    uge_port = config.getUGE_Port()[cluster_inx]
    jobid = sys.argv[1]
    taskid = None
    if len(sys.argv) > 2:
        taskid = sys.argv[2]
    jobinfo = UGERest.getUGEJobInfo(uge_ip, uge_port, jobid, taskid)

    for attr in [atr for atr in dir(jobinfo) if (not atr.startswith('__'))
                                              and (not callable(getattr(jobinfo, atr)))]:
        print(attr + " --> " + str(getattr(jobinfo, attr)))
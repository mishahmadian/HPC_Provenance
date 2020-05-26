# -*- coding: utf-8 -*-
"""
    This module handles all the communication with Univa Grid engine (UGE) Job Scheduler.
    In this module the required data corresponding to job will be recieved from:
        - UGE RESTFull API from the UGE Qmaster node
        - Accounting Data from Unisight MongoDB

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from multiprocessing import Process, Queue, Event
from communication import ServerConnection
from config import ServerConfig
#from scheduler import UGEJobInfo
import json, urllib.request
from pathlib import Path
from typing import List
import scheduler
#
# This class communicates with UGERestful API in order to collect required information
class UGE:
    def __init__(self, shutdown : Event):
        self.config = ServerConfig()
        # RPC Request dict: {cluster1 : [list of jobIds 1], cluster2 : [list of jobIds 2], ...}
        self.ugeAcctJobIdReq_Q = Queue()
        # RPC Response dict: {cluster1 : [list of jobInfo 1], cluster2 : [list of jobInfo 2], ...}
        self.ugeAcctJobInfoRes_Q = Queue()
        # Process Event control
        self.timeout = Event()
        # Shutdown Event
        self.shutdown = shutdown
        # Start the UGE Accounting RPC process if [uge] section is defined in server.conf
        self._ugeAcctProc: Process = Process(target=self.__UGE_Accounting_Service,
                                             args=(self.ugeAcctJobIdReq_Q, self.ugeAcctJobInfoRes_Q,))
        # Start the UGE Accounting Process
        self._ugeAcctProc.daemon = True
        self._ugeAcctProc.start()

    @staticmethod
    def getUGERestJobInfo(ip_addr, port, jobid, taskId) -> 'scheduler.UGEJobInfo' or None:
        # Define the UGERest base URL
        ugerest_base_url = "http://" + ip_addr + ":" + port
        # UGERest JobInfo API URL
        ugerest_job_url = ugerest_base_url + "/jobs/" + str(jobid) + ("." + str(taskId) if taskId else ".1")
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
                jobInfo.jobid = jobid
                jobInfo.taskid = taskId
                jobInfo.sched_type = 'uge'
                jobInfo.status = scheduler.UGEJobInfo.Status(
                    {'r' : 1, 'qw' : 2, 'd' : 3, 'dr' : 3, 'E' : 4, 'Eqw' : 4}.get(data['state'], 5)
                )
                jobInfo.jobName = data['name']
                jobInfo.queue, jobInfo.exec_host = data['queue'].split('@')
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
    # Close the scheduler by terminating the internal process
    #
    # def close(self):
    #     if self._ugeAcctProc:
    #         self._ugeAcctProc.terminate()

    #
    # Find and return the job submission script of the given job
    #
    def getUGE_JobScript(self, jobinfo: 'scheduler.UGEJobInfo'):

        jobid, exec_host, work_dir, cmd = jobinfo.jobid, jobinfo.exec_host, jobinfo.pwd, jobinfo.command
        # Do not proceed for the following types of job
        if cmd and any(q_cmd in cmd.lower() for q_cmd in ["qlogin", "qrsh"]):
            return "-1" ## This job has no script file
            # The requested cluster must exist in server.conf

        uge_clusters = self.config.getUGE_clusters()
        if jobinfo.cluster and jobinfo.cluster in uge_clusters:
            cluster_inx = uge_clusters.index(jobinfo.cluster)
        else:
            raise UGEServiceException("Configurations specs for [" + jobinfo.cluster + "] "
                                         "cannot be found in server.conf")

        # The location of UGE Spool Directory for this 'cluster'

        uge_spool_dir = self.config.getUGE_spool_dirs()[cluster_inx]
        # First check the spool directory for the script file. That's the most reliable version
        if uge_spool_dir and exec_host:
            exec_host = exec_host.split('.')[0]
            # Find the spool directory under
            uge_job_script = Path(uge_spool_dir).joinpath(exec_host).joinpath("job_scripts").joinpath(jobid)
            if not uge_job_script.exists():
                # Sometimes UGE creates an strange spool directory hierarchy
                uge_job_script = Path(uge_spool_dir).joinpath(exec_host).joinpath(exec_host)\
                    .joinpath("job_scripts").joinpath(jobid)
                if not uge_job_script.exists():
                    uge_job_script = None

            # Get job submission script content
            if uge_job_script:
                with open(uge_job_script.as_posix(), mode='r') as jobScriptF:
                    return jobScriptF.read()

        # If job submission script was not available in UGE Spool directory, then it means
        # the job has been finished and may be able to find the script file under user directory
        if work_dir and cmd:
            script_file = cmd.split()[-1]
            # Look into user's directory and try to find the script file
            uge_job_script = Path(work_dir).joinpath(script_file)
            if uge_job_script.exists():
                with open(uge_job_script.as_posix(), mode='r') as jobScriptF:
                    return jobScriptF.read()

        return None


    @staticmethod
    def __getUGEAcctJobInfo(cluster, job_task_id_lst : List[str]) -> List['scheduler.UGEJobInfo']:
        # Define the RabbitMQ RPC queue that calls remote UGE Accounting collector
        rpc_queue = '_'.join([cluster, 'rpc', 'queue'])
        # Establish a RPC Connection
        serverCon = ServerConnection(is_rpc=True)
        # assemble the jobId and taskId
        jobInfoLst : List['scheduler.UGEJobInfo'] = []
        # Generates an RPC Request:
        #   - action: what type of actions should RPC call does
        #   - data: a list of data that has to be passed to that particular action
        request = json.dumps({'action' : 'uge_acct', 'data' : job_task_id_lst})
        # Call the server RPC and receive the response as String\
        response = serverCon.rpc_call(rpc_queue, request)

        if response.strip() != 'NONE':
            # The response from UGE Accounting server would be a large string separated by [^@]
            jobRecLst = response.split('[^@]')
            # Iterate over each record in accounting data from UGE server
            for jobRec in jobRecLst:
                # in UGE Accounting file the fields are separated by ':'
                jobRecArray = jobRec.split(':')
                # Make a UGEJobInfo Object for this record
                jobInfo = scheduler.UGEJobInfo()
                jobInfo.jobid = jobRecArray[5]
                jobInfo.cluster = cluster
                jobInfo.sched_type = 'uge'
                jobInfo.taskid = (jobRecArray[35] if int(jobRecArray[35]) else None)
                jobInfo.status = scheduler.UGEJobInfo.Status.FINISHED
                jobInfo.jobName = jobRecArray[4]
                jobInfo.queue = jobRecArray[0]
                jobInfo.exec_host = jobRecArray[1]
                jobInfo.num_cpu = jobRecArray[34]
                jobInfo.submit_time = jobRecArray[8]
                jobInfo.start_time = jobRecArray[9]
                jobInfo.end_time = jobRecArray[10]
                jobInfo.username = jobRecArray[3]
                jobInfo.parallelEnv = jobRecArray[33]
                jobInfo.project = jobRecArray[31]
                jobInfo.pwd = jobRecArray[50]
                jobInfo.command = jobRecArray[51]
                jobInfo.cpu = jobRecArray[36]
                jobInfo.io = jobRecArray[38]
                jobInfo.ioops = jobRecArray[53]
                jobInfo.iow = jobRecArray[40]
                jobInfo.maxvmem = jobRecArray[42]
                jobInfo.mem = jobRecArray[37]
                jobInfo.wallclock = jobRecArray[52]
                jobInfo.failed_no = jobRecArray[11]
                if jobRecArray[46].strip() != 'NONE':
                    jobInfo.q_del.extend(jobRecArray[46].split(','))

                for option in jobRecArray[39].split():
                    if 'h_vmem=' in option:
                        jobInfo.h_vmem = option.split('=')[1].strip()
                    if 'h_rt=' in option:
                        jobInfo.h_rt = option.split('=')[1].strip()
                    if 's_rt=' in option:
                        jobInfo.s_rt = option.split('=')[1].strip()

                # Append the UGEJobInfo object into the list
                jobInfoLst.append(jobInfo)

        return jobInfoLst

    #
    # In case of using Univa Grid Engine (UGE) a process should be spawned
    # to keep contacting the UGE q_master and collecting the accounting data
    # for already finished jobs. Since reading the "accounting" file is an expensive
    # process, a separate process in an specific intervals will take care of that.
    #
    def __UGE_Accounting_Service(self, acctJobIdReq_Q : Queue, acctJobInfoRes_Q : Queue):
        # Get the waiting time
        wait_Intv = self.config.getUGEAcctRPCIntv()
        while not (self.timeout.is_set() or self.shutdown.is_set()):
            job_req_list = []
            # get all job_ids in one snapshot
            while acctJobIdReq_Q.qsize():
                job_req_list.append(acctJobIdReq_Q.get())
            # Map the cluster_jobId to a dict={cluster : [jobId...]}
            job_req_map = {}
            for item in job_req_list:
                cluster, job_task_id = item.split('_')
                if not job_req_map.get(cluster):
                    job_req_map[cluster] = [job_task_id]
                else :
                    job_req_map[cluster].append(job_task_id)

            # for each cluster calls the UGE Accounting remote RPC call and
            # put the responses (list of JobInfo) in acctJobInfoRes_Q
            for cluster in job_req_map.keys():
                # Call UGE Accounting RPC
                jobInfoLst: List['scheduler.UGEJobInfo'] = \
                    self.__getUGEAcctJobInfo(cluster, job_req_map.get(cluster))
                # Put items in Queue
                for jobInfo in jobInfoLst:
                    acctJobInfoRes_Q.put(jobInfo)

            # Wait the amount of time that is defined under [uge] section
            try:
                self.timeout.wait(wait_Intv)
            except:
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
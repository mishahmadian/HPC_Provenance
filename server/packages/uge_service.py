# -*- coding: utf-8 -*-
"""
    This module handles all the communication with Univa Grid engine (UGE) Job Scheduler.
    In this module the required data corresponding to job will be recieved from:
        - UGE RESTFull API from the UGE Qmaster node
        - Accounting Data from Unisight MongoDB

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from config import ServerConfig, ConfigReadExcetion
import json, urllib.request
import sys

class UGE:
    @staticmethod
    def getUGEJobInfo(ip_addr, port, cluster, jobId):
        UGE.__collectUGErest(ip_addr, port, jobId)


    @staticmethod
    def __collectUGErest(ip_addr, port, jobId):

        ugerest_base_url = "http://" + ip_addr + ":" + port
        ugerest_job_url = ugerest_base_url + "/jobs/" + str(jobId)

        response = urllib.request.urlopen(ugerest_job_url)
        # response = urllib.request.urlopen(ugerest_base_url + "/hostsummary/compute-14-15.localdomain")
        # r = urllib.request.urlopen(ugerest_base_url + "/jobs_for_user/*/r")
        # r = urllib.request.urlopen("http://10.100.21.254:8182/hostsummary/1/100")
        if response:
            data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
            if 'errorCode' in data and data['errorCode'] == 299:
                print("No Data")
            else:
                print(json.dumps(data, sort_keys=True, indent=4))

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
    UGE.getUGEJobInfo(uge_ip, uge_port, 'genius', jobid)
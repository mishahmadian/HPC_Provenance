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

class UGE:
    @staticmethod
    def getUGEJobInfo(config, cluster, iobId):
        r = urllib.request.urlopen("http://10.100.21.254:8182/jobs/952798.470")
        # r = urllib.request.urlopen("http://10.100.21.254:8182/jobs_for_user/*/r")
        # r = urllib.request.urlopen("http://10.100.21.254:8182/hostsummary/1/100")
        data = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))
        print(json.dumps(data, sort_keys=True, indent=4))
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
        pass
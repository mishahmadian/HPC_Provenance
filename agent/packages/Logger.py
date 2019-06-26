# -*- coding: utf-8 -*-
"""
    Log the activities into "../logs/":

        - Errors
        - Signals
        - Activities

    Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from packages.Config import AgentSettings, ConfigReadExcetion
import os

class Logger:
    def __init__(self):
        self.__LOG_DIR = "../logs"

        # Import agent.conf settings
        self.__config = AgentConfig()
        # Create logs directory if it does not exist
        if not os.path.exists(self.__LOG_DIR):
            os.makedirs(self.__LOG_DIR)

        with open(self.__LOG_DIR/)

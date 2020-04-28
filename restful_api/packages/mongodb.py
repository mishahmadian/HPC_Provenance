# -*- coding: utf-8 -*-
"""
    This module manages the Access to the Provenance MongoDB instance
    and provides all sets of required queries

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from api_config import Config, ConfigReadExcetion
from pymongo.errors import PyMongoError
from api_logger import log, Mode
from pymongo import MongoClient

class MongoDB:
    def __init__(self):
        try:
            config = Config()
            # Initialize connection to MongoDB Server
            self._mongoClient = MongoClient(host=config.getMongoHost(),
                                           port=config.getMongoPort(),
                                           document_class=dict,
                                           username=config.getMongoUser(),
                                           password=config.getMongoPass(),
                                           authSource=config.getMongoSrcDB(),
                                           authMechanism=config.getMongoAuthMode())
            # Use Provenance Database
            self._mongoDB = self._mongoClient[config.getMongoSrcDB()]

        except ConfigReadExcetion as confExp:
            log(Mode.MONGODB, confExp.getMessage())

        except PyMongoError as mongoExp:
            log(Mode.MONGODB, str(mongoExp))


    def query_oss_jobs(self):
        """
            Aggregate the OSS/OST data with their corresponding jobs
        :return:
        """
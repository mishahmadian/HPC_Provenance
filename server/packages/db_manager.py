# -*- coding: utf-8 -*-
"""
    This module creates and manages the Database instances and provides supported functionalities
    Databases:
        - MongoDB
        - InfluxDB
"""
from config import ServerConfig, ConfigReadExcetion
from pymongo.errors import PyMongoError
from typing import Union, Dict, List
from pymongo import MongoClient
from enum import Enum, unique

class MongoDB:
    """
     This class will take care of any interaction with MongoDB server
     The [mongodb] section in server.conf file should be completed
    """
    def __init__(self):
        try:
            config = ServerConfig()
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
            raise confExp

        except PyMongoError as mongoExp:
            raise DBManagerException(str(mongoExp))

    #
    # Main insert method for Mongo DB
    #
    def insert(self, collection: 'MongoDB.Collections', data: Union[Dict, List]) -> None:
        """
        Intest a list or dictionary of data into the selected collection od Provenance database

        :param collection: MongoDB.Collections
        :param data: Dict | List
        :return: None
        """
        if not isinstance(collection, self.Collections):
            raise DBManagerException("(insert) The type of 'Collection' is wrong"
                                     , DBManagerException.DBType.MONGO_DB)

        if not (isinstance(data, Dict) or isinstance(data, List)):
            raise DBManagerException("(insert)The type of 'data' must be 'Dict | List'"
                                     , DBManagerException.DBType.MONGO_DB)

        try:
            # Selects the collection of this database
            coll = self._mongoDB[collection.value]
            # Insert data (document) into the collection
            # If data is Dict:
            if isinstance(data, Dict):
                coll.insert_one(data)

            # Otherwise:
            if isinstance(data, List):
                coll.insert_many(data)

        except PyMongoError as mongoExp:
            raise DBManagerException(f"(insert) {str(mongoExp)}", DBManagerException.DBType.MONGO_DB)

    #
    # Main Update Method for MongoDB
    #
    def update(self, collection: 'MongoDB.Collections', doc_query: Dict, data: Dict) -> bool:
        """
        Update a document selected by doc_query with new data

        :param collection: MongoDB.Collections
        :param doc_query: Dict
        :param data: Dict
        :return: Boolean
        """
        if not isinstance(collection, self.Collections):
            raise DBManagerException("(update) The type of 'Collection' is wrong"
                                     , DBManagerException.DBType.MONGO_DB)

        if not isinstance(data, Dict):
            raise DBManagerException("(update) The type of 'data' must be 'Dict'"
                                     , DBManagerException.DBType.MONGO_DB)

        if not isinstance(doc_query, Dict):
            raise DBManagerException("(update) The type of 'doc_query' must be 'Dict'"
                                     , DBManagerException.DBType.MONGO_DB)

        try:
            # Selects the collection of this database
            coll = self._mongoDB[collection.value]
            # Set the new data values
            newData = {"$set" : data}
            # update the database
            result = coll.update_one(doc_query, newData, upsert=True)
            # Return the update success
            return result.modified_count > 0

        except PyMongoError as mongoExp:
            raise DBManagerException(f"(update) {str(mongoExp)}", DBManagerException.DBType.MONGO_DB)

    #
    # Close The MongoDB connection
    #
    def close(self):
        """
        Close Mongo Client
        :return: None
        """
        try:
            self._mongoClient.close()

        except PyMongoError as mongoExp:
            raise DBManagerException(f"(close) {str(mongoExp)}", DBManagerException.DBType.MONGO_DB)

    def test(self):
        print("collections: " + str(self._mongoDB.list_collection_names()))

    #
    # MongoDB Collection Enum
    #
    @unique
    class Collections(Enum):
        """
            The available collections for this MondoDB are defined in this Enum class
        """
        JOB_INFO_COLL = 'jobinfo'
        MDS_STATS_COLL = 'mds_stats'
        OSS_STATS_COLL = 'oss_stats'
        FILE_OP_COLL = 'file_op'


#
# In case of error the following exception can be raised
#
class DBManagerException(Exception):
    def __init__(self, message: str, expType: 'DBManagerException.DBType'):
        super(DBManagerException, self).__init__(message)
        self.message = message
        self.expType = expType

    def getMessage(self):
        return "\n [Error] _DBManager_[{}]_: {} \n".format(self.expType.name, self.message)

    #
    # DBManagerException might be caused by different DB instances
    #
    class DBType(Enum):
        """
        All the supported Databases:
        """
        MONGO_DB = 1
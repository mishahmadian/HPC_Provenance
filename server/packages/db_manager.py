# -*- coding: utf-8 -*-
"""
    This module creates and manages the Database instances and provides supported functionalities
    Databases:
        - MongoDB
        - InfluxDB
        - Neo4j
"""
from pymongo import MongoClient, IndexModel, TEXT, ASCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError
from config import ServerConfig, ConfigReadExcetion
from typing import Union, Dict, List
from logger import log, Mode
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
            log(Mode.DB_MANAGER, confExp.getMessage())

        except PyMongoError as mongoExp:
            log(Mode.DB_MANAGER, str(mongoExp))

    #
    # Prepare Database when application starts running
    #
    def prepare(self) -> None:
        """
            This method should be called when the program gets started
            to prepare the database by performing some actions such as:
            - Create specific Indexes for collections
        :return:
        """
        for collection in self.Collections:
            # Get all available collections of this database
            allcolls = self._mongoDB.collection_names()
            # Create Index for JobInfo
            if collection.value == "jobinfo":
                # If JobInfo is not created yet
                if not collection.value in allcolls:
                    coll = self._mongoDB[collection.value]
                    # Define the Index(es)
                    uid_uniq_inx = IndexModel([("uid", TEXT), ("jobid", ASCENDING)],
                                              name="jobinfo_uid_uniq_inx", unique=True)
                    # Add/Create indexes
                    coll.create_indexes([uid_uniq_inx])
                    #
                    log(Mode.DB_MANAGER, "The 'jobinfo' collection was created and Indexed")


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
                if data:
                    coll.insert_one(data)

            # Otherwise:
            if isinstance(data, List):
                if data:
                    coll.insert_many(data)

        except DuplicateKeyError as dupExp:
            raise dupExp

        except PyMongoError as mongoExp:
            raise DBManagerException(f"(insert) {str(mongoExp)}", DBManagerException.DBType.MONGO_DB)

    #
    # Main Update Method for MongoDB
    #
    def update(self, collection: 'MongoDB.Collections', doc_query: Dict, data: Union[Dict, List]) -> bool:
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

        if not (isinstance(data, Dict) or isinstance(data, List)):
            raise DBManagerException("(update) The type of 'data' must be 'Dict | List'"
                                     , DBManagerException.DBType.MONGO_DB)

        if not isinstance(doc_query, Dict):
            raise DBManagerException("(update) The type of 'doc_query' must be 'Dict'"
                                     , DBManagerException.DBType.MONGO_DB)

        try:
            # Selects the collection of this database
            coll = self._mongoDB[collection.value]
            # Set the new data values
            newData = {"$set" : data}
            # update the database if both query and data are defined
            if doc_query and data:
                result = coll.update_one(doc_query, newData, upsert=True)
                # Return the update success
                return result.modified_count > 0
            else:
                return False

        except DuplicateKeyError as dupExp:
            raise dupExp

        except PyMongoError as mongoExp:
            raise DBManagerException(f"(update) {str(mongoExp)}", DBManagerException.DBType.MONGO_DB)

    #
    # Query method for MongoDB that returns the first found document
    #
    def find_one(self, collection: 'MongoDB.Collections', query: Dict):
        """
        Find the first queried documents based on "query" criteria
        Returns none or one document
        :param collection: MongoDB.Collections
        :param query: Dict
        :return: Dict
        """
        if not isinstance(collection, self.Collections):
            raise DBManagerException("(query) The type of 'Collection' is wrong"
                                     , DBManagerException.DBType.MONGO_DB)

        if not isinstance(query, Dict):
            raise DBManagerException("(query) The type of 'query' must be 'Dict'"
                                     , DBManagerException.DBType.MONGO_DB)

        # the query should be defined and not empty
        if not query:
            return None

        # Selects the collection of this database
        coll = self._mongoDB[collection.value]
        # Find the query data in database
        docs = coll.find_one(query)
        # Return the found documents
        return docs


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
        #print("collections: " + str(self._mongoDB.list_collection_names()))
        colls = self._mongoDB.collection_names()
        if not "jobinfo2" in colls:
            print("Creating Index")
            coll = self._mongoDB['jobinfo2']
            uid_index = IndexModel([("uid", HASHED)], name="uid_uniq_inx", unique=True)
            coll.create_indexes([uid_index])

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
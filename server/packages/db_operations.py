# -*- coding: utf-8 -*-
"""
    This module performs the database operations that are required to dump, retrieve, or query
    data from different databases such as:
        - MongoDB
        - InfluxDB
        - Neo4J
"""
from db_manager import MongoDB, DBManagerException
from pymongo.errors import DuplicateKeyError
from logger import log, Mode
import aggregator as aggr
from typing import Dict

class MongoOPs:
    """
        In This class all the Database Operations that has to be done
        on MongoDB is defined here.
    """
    #
    # Dump to MongoDB database
    #
    @staticmethod
    def dump2MongoDB(provenData: Dict[str, 'aggr.ProvenanceObj']):
        """
        Store the aggregated Provenance Data into MongoDB
            * Should be running as a separate process

        :param provenData: Dict of ProvenanceObj
        :return: None
        """
        # define mongodb
        mongodb = None
        try:
            mongodb = MongoDB()
            # Update the Provenance Objects in MongoDb. If they do not exist,
            # then they will be inserted into db as a new document
            for uid, provenObj in provenData.items():
                # Update/Insert JobInfo
                jobinfo = provenObj.jobInfo
                # Insert/Update one job per document, Ignore JobInfos after the jobs is finished
                update_query = {'uid' : uid, 'status' : {"$ne" : "FINISHED"}}
                try:
                    mongodb.update(MongoDB.Collections.JOB_INFO_COLL,
                                   update_query,
                                   jobinfo.to_dict())

                # Ignore the duplicate key error after the job is finished
                except DuplicateKeyError:
                    pass

                # Update/Insert last collected MDS data
                mdsObj = provenObj.MDSDataObj_lst[-1] if provenObj.MDSDataObj_lst else None
                if mdsObj:
                    # Insert/Update per each job running on each MDT of each MDS
                    update_query = {'uid': uid, 'mds_host': mdsObj.mds_host, 'mdt_target': mdsObj.mdt_target}
                    mongodb.update(MongoDB.Collections.MDS_STATS_COLL,
                                   update_query,
                                   mdsObj.to_dict())

                # Update/Insert last collected OSS data
                ossObj = provenObj.OSSDataObj_lst[-1] if provenObj.OSSDataObj_lst else None
                if ossObj:
                    # Insert/Update per each job running on each OST of each OSS
                    update_query = {'uid': uid, 'mds_host': ossObj.oss_host, 'mdt_target': ossObj.ost_target}
                    mongodb.update(MongoDB.Collections.OSS_STATS_COLL,
                                   update_query,
                                   ossObj.to_dict())

                # Insert collected File Operations data (No update)
                fopObj = provenObj.FileOpObj_lst
                fopObj_lst = []
                # Collect all file operations and insert all at once
                for fopData in fopObj:
                    fopObj_lst.append(fopData.to_dict())
                # Insert all the fop object in one operation
                mongodb.insert(MongoDB.Collections.FILE_OP_COLL, fopObj_lst)

        except DBManagerException as dbExp:
            log(Mode.AGGREGATOR, dbExp.getMessage())

        finally:
            # Close the MongoClient
            print("=== Dumped to MongoDB ===")
            if mongodb:
                mongodb.close()
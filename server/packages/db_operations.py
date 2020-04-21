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
                #============================ INSERT/UPDATE JobInfo===================================
                # Insert/Update one job per document, Ignore JobInfos after the jobs is finished
                update_query = {'uid' : uid, 'status' : {"$ne" : "FINISHED"}}
                try:
                    mongodb.update(MongoDB.Collections.JOB_INFO_COLL,
                                   update_query,
                                   jobinfo.to_dict())

                # Ignore the duplicate key error after the job is finished
                except DuplicateKeyError:
                    pass

                #============================ INSERT/UPDATE MDS Data =================================
                # Get a table of latest MDS_MDT data objects
                mdsObj_tbl = provenObj.get_MDS_table()
                # Update/Insert last collected MDS data
                if mdsObj_tbl:
                    # Update/Insert per MDS and MDT
                    for mds_host, mds in mdsObj_tbl.items():
                        for mdt_target, mdsObj in mds.items():
                            print(f"-------------- MDS   Snapshot_time: {mdsObj.snapshot_time}  Open: {mdsObj.open}  "
                                  f"close: {mdsObj.close}")
                            # Insert/Update the running job on each MDT of each MDS
                            update_query = {'uid': uid, 'mds_host': mds_host, 'mdt_target': mdt_target}
                            mongodb.update(MongoDB.Collections.MDS_STATS_COLL,
                                           update_query,
                                           mdsObj.to_dict())

                #============================ INSERT/UPDATE OSS Data =================================
                # Get a table of latest OSS_OST data objects
                ossObj_tbl = provenObj.get_OSS_table()
                # Update/Insert last collected OSS_MDS data
                if ossObj_tbl:
                    # Update/Insert per OSS and OST
                    for oss_host, ost_tble in ossObj_tbl.items():
                        for ost_target, ossObj in ost_tble.items():
                            print(f"-------------- OSS: {ossObj.oss_host}  Snapshot_time: {ossObj.snapshot_time}  "
                                  f"Max_write: {ossObj.write_bytes_max}  Min_Write: {ossObj.write_bytes_min}  "
                                  f"Sum_Write: {ossObj.write_bytes_sum}")
                            # Insert/Update per each job running on each OST of each OSS
                            update_query = {'uid': uid, 'oss_host': oss_host, 'oss_target': ost_target}
                            mongodb.update(MongoDB.Collections.OSS_STATS_COLL,
                                           update_query,
                                           ossObj.to_dict())

                # ============================ INSERT/UPDATE File OPs ================================
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
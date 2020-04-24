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
from scheduler import UGEJobInfo
from datetime import datetime
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
                # First check the JobInfo type. The JobInfo should be a subclass of the original JobInfo
                # Otherwise, it's not "complete". In some rare cases the JobInfo gets created but before
                # gets a chance to be filled out by the "JobScheduler" class, it comes here to be stored
                # in database. That should be ignored!
                if jobinfo and isinstance(jobinfo, UGEJobInfo): # 'or' isinstance(Slurm...)
                    # Insert/Update one job per document, Ignore JobInfos after the jobs is finished
                    update_query = {'uid' : uid, 'status' : {"$ne" : "FINISHED"}}
                    try:
                        # Make the update request
                        jobinfo_update = {
                            "$set" : jobinfo.to_dict(),
                            # Add create_time upon first insertion
                            "$setOnInsert" : {"create_time" : datetime.utcnow()},
                            # Add modified_time upon each update
                            "$currentDate" : {"modified_time" : True}
                        }

                        mongodb.update(MongoDB.Collections.JOB_INFO_COLL,
                                       update_query,
                                       jobinfo_update)

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
                            # Insert/Update the running job on each MDT of each MDS
                            # Create Update query
                            update_query = {'uid': uid, 'mds_host': mds_host, 'mdt_target': mdt_target}

                            # Modify the ossObj to gets updated properly
                            mdsObj_doc = mdsObj.to_dict()

                            # Add up these fields of current data with those already stored if
                            # snapshot_time has been changed since last data aggregation
                            sum_fields = ["open", "close", "mknod", "link", "unlink", "mkdir", "rmdir", "rename",
                                          "getattr", "setattr", "samedir_rename", "crossdir_rename"]
                            for sum_field in sum_fields:
                                mdsObj_doc[sum_field] = {
                                    "$sum": [
                                        f"${sum_field}",
                                        {"$cond": [{"$ne": ["$snapshot_time", mdsObj_doc.get("snapshot_time")]},
                                                   mdsObj_doc.get(sum_field), 0]}
                                    ]
                                }
                            # Add create_time upon the first insertion
                            mdsObj_doc["create_time"] = {
                                "$cond": [{"$not": ["$create_time"]}, "$$NOW", "$create_time"]
                            }
                            # Add modified_time whenever the field gets update
                            mdsObj_doc["modified_time"] = "$$NOW"

                            # Specify the update document request
                            mds_update_req = [{"$set": mdsObj_doc}]

                            mongodb.update(MongoDB.Collections.MDS_STATS_COLL,
                                           update_query, mds_update_req,
                                           runcommand=True)

                #============================ INSERT/UPDATE OSS Data =================================
                # Get a table of latest OSS_OST data objects
                ossObj_tbl = provenObj.get_OSS_table()
                # Update/Insert last collected OSS_MDS data
                if ossObj_tbl:
                    # Update/Insert per OSS and OST
                    for oss_host, ost_tble in ossObj_tbl.items():
                        for ost_target, ossObj in ost_tble.items():
                            # Insert/Update per each job running on each OST of each OSS
                            # Create Update query
                            update_query = {'uid': uid, 'oss_host': oss_host, 'oss_target': ost_target}

                            # Modify the ossObj to gets updated properly
                            ossObj_doc = ossObj.to_dict()
                            # Choose the Max value between these current data and already stored ones
                            for max_field in ["read_bytes_max", "write_bytes_max"]:
                                ossObj_doc[max_field] = {
                                    "$max" : [f"${max_field}", ossObj_doc.get(max_field)]
                                }
                            # Choose the Min value between these current data and already stored ones
                            for min_field in ["read_bytes_min", "write_bytes_min"]:
                                ossObj_doc[min_field] = {
                                    "$min" : [f"${min_field}", ossObj_doc.get(min_field)]
                                }
                            # Add up these fields of current data with those already stored if
                            # snapshot_time has been changed since last data aggregation
                            sum_fields = ["read_bytes", "write_bytes", "read_bytes_sum", "write_bytes_sum",
                                          "getattr", "setattr", "punch", "sync", "destroy", "create"]
                            for sum_field in sum_fields:
                                ossObj_doc[sum_field] = {
                                    "$sum": [
                                        f"${sum_field}",
                                        {"$cond": [{"$ne": ["$snapshot_time", ossObj_doc.get("snapshot_time")]},
                                                   ossObj_doc.get(sum_field), 0]}
                                    ]
                                }
                            # Add create_time upon the first insertion
                            ossObj_doc["create_time"] = {
                                "$cond": [{"$not": ["$create_time"]}, "$$NOW", "$create_time"]
                            }
                            # Add modified_time whenever the field gets update
                            ossObj_doc["modified_time"] = "$$NOW"

                            # Specify the update document request
                            oss_update_req = [{"$set" : ossObj_doc}]

                            mongodb.update(MongoDB.Collections.OSS_STATS_COLL,
                                           update_query, oss_update_req,
                                           runcommand= True)

                # ============================ INSERT/UPDATE File OPs ================================
                # Insert collected File Operations data (No update)
                fopObj = provenObj.FileOpObj_lst
                fopObj_lst = []
                # Collect all file operations and insert all at once
                for fopData in fopObj:
                    fopData_doc = fopData.to_dict()
                    # Add create_time for each record
                    fopData_doc["create_time"] = datetime.utcnow()
                    fopObj_lst.append(fopData_doc)
                # Insert all the fop object in one operation
                mongodb.insert(MongoDB.Collections.FILE_OP_COLL, fopObj_lst)

        except DBManagerException as dbExp:
            log(Mode.AGGREGATOR, dbExp.getMessage())

        finally:
            # Close the MongoClient
            #print("=== Dumped to MongoDB ===")
            if mongodb:
                mongodb.close()
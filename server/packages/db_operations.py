# -*- coding: utf-8 -*-
"""
    This module performs the database operations that are required to dump, retrieve, or query
    data from different databases such as:
        - MongoDB
        - InfluxDB
        - Neo4J
"""
from db_manager import MongoDB, InfluxDB, DBManagerException
from pymongo.errors import DuplicateKeyError
from scheduler import UGEJobInfo, JobInfo
from multiprocessing import Queue
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
                # only store data for those data that have JobInfo (e.g. ignore process-only jobs
                if provenObj.jobInfo:
                    #============================ INSERT/UPDATE MDS Data =================================
                    # Get a table of latest MDS_MDT data objects
                    mdsObj_tbl = provenObj.get_MDS_table()
                    # Update/Insert last collected MDS data
                    if mdsObj_tbl:
                        # Update/Insert per MDS and MDT
                        for mds_host, mds in mdsObj_tbl.items():
                            for mdt_target, mdsObj in mds.items():
                                # Get mdsObj in dictionary format
                                mdsObj_doc = mdsObj.to_dict()
                                # ----------------- Perform Update an mds_info Element ------------------
                                # update the document if the data for current mds_host
                                # and mdt_target is already available in database
                                mds_update_query = {
                                    'uid': uid,
                                    'mds_info.mds_host': mds_host,
                                    'mds_info.mdt_target': mdt_target
                                }

                                # Create Update document
                                mds_update_doc = {
                                    # Update snapshot_time and timestamp per MDS/MDT
                                    "$set": {
                                        "mds_info.$.snapshot_time": mdsObj_doc.get('snapshot_time'),
                                        "mds_info.$.timestamp": mdsObj_doc.get('timestamp')
                                    },
                                    # Add up these fields of current MDS data with those already stored
                                    "$inc": {
                                        f"mds_info.$.{sum_fields}": mdsObj_doc.get(sum_fields) for sum_fields in
                                                ["open", "close", "mknod", "link", "unlink", "mkdir", "rmdir", "rename",
                                                 "getattr", "setattr", "samedir_rename", "crossdir_rename", "statfs"]
                                    },
                                    "$currentDate": {
                                        "last_modified": True,
                                        "mds_info.$.modified_time": True
                                    }
                                }
                                result = mongodb.update(MongoDB.Collections.MDS_STATS_COLL,
                                                        mds_update_query, mds_update_doc,
                                                        upsert=False)

                                # ------------- Perform Insert MDS Data or an element inmds_info ----------------
                                # If No Update happened then it means either the data for this uid
                                # does not exist or the data element for this mds_host/mdt_target
                                # has not been entered yet
                                if result == MongoDB.Result.NO_CHANGE:
                                    # we only add element to the current data or insert a new one
                                    mds_upsert_query = {'uid': uid}

                                    # Add some extra fields (status, username, modified_time)
                                    mdsObj_doc["modified_time"] = datetime.now()
                                    mdsObj_doc["status"] = None
                                    mdsObj_doc["username"] = None

                                    # Create update/insert document
                                    mds_upsert_doc = {
                                        "$set": {
                                            data_field: mdsObj_doc.pop(data_field) for data_field in
                                            ["uid", "jobid", "taskid", "cluster", "sched_type",
                                             "procid", "status", "username"]
                                        },
                                        # Add the new element into 'mds_info'
                                        "$push": {
                                            "mds_info": mdsObj_doc
                                        },
                                        # Add create_time and last_modified
                                        "$currentDate": {
                                            "create_time": True,
                                            "last_modified": True
                                        }
                                    }

                                    mongodb.update(MongoDB.Collections.MDS_STATS_COLL,
                                                   mds_upsert_query, mds_upsert_doc,
                                                   upsert=True)

                    #============================ INSERT/UPDATE OSS Data =================================
                    # Get a table of latest OSS_OST data objects
                    ossObj_tbl = provenObj.get_OSS_table()
                    # Update/Insert last collected OSS_MDS data
                    if ossObj_tbl:
                        # Update/Insert per OSS and OST
                        for oss_host, ost_tble in ossObj_tbl.items():
                            for ost_target, ossObj in ost_tble.items():
                                # Get ossObj in dictionary format
                                ossObj_doc = ossObj.to_dict()
                                #----------------- Perform Update an oss_info Element ------------------
                                # update the document if the data for current oss_host
                                # and oss_target is already available in database
                                oss_update_query = {
                                    'uid': uid,
                                    'oss_info.oss_host': oss_host,
                                    'oss_info.ost_target': ost_target
                                }

                                # Create Update document
                                oss_update_doc = {
                                    # Update snapshot_time and timestamp per OSS/OST
                                    "$set": {
                                        "oss_info.$.snapshot_time": ossObj_doc.get('snapshot_time'),
                                        "oss_info.$.timestamp": ossObj_doc.get('timestamp')
                                    },
                                    # Choose the Max value between these current data and already stored ones
                                    "$max": {
                                        f"oss_info.$.{max_field}": ossObj_doc.get(max_field) for max_field in
                                                                        ["read_bytes_max", "write_bytes_max"]
                                    },
                                    # Choose the Min value between these current data and already stored ones
                                    "$min": {
                                        f"oss_info.$.{min_field}": ossObj_doc.get(min_field) for min_field in
                                                                        ["read_bytes_min", "write_bytes_min"]
                                    },
                                    # Add up these fields of current data with those already stored
                                    "$inc": {
                                        f"oss_info.$.{sum_fields}": ossObj_doc.get(sum_fields) for sum_fields in
                                                ["read_bytes", "write_bytes", "read_bytes_sum", "write_bytes_sum",
                                                  "getattr", "setattr", "punch", "sync", "destroy", "create"]
                                    },
                                    "$currentDate": {
                                        "last_modified": True,
                                        "oss_info.$.modified_time": True
                                    }
                                }
                                result = mongodb.update(MongoDB.Collections.OSS_STATS_COLL,
                                                        oss_update_query, oss_update_doc,
                                                        upsert=False)

                                # ------------- Perform Insert OSS Data or an element in oss_info ----------------
                                # If No Update happened then it means either the data for this uid
                                # does not exist or the data element for this oss_host/ost_target
                                # has not been entered yet
                                if result == MongoDB.Result.NO_CHANGE:
                                    # we only add element to the current data or insert a new one
                                    oss_upsert_query = {'uid': uid}

                                    # Add some extra fields (status, username, modified_time)
                                    ossObj_doc["modified_time"] = datetime.now()
                                    ossObj_doc["status"] = None
                                    ossObj_doc["username"] = None

                                    # Create update/insert document
                                    oss_upsert_doc = {
                                        "$set": {
                                            data_field: ossObj_doc.pop(data_field) for data_field in
                                                    ["uid", "jobid", "taskid", "cluster", "sched_type",
                                                     "procid", "status", "username"]
                                        },
                                        # Add the new element into 'oss_info'
                                        "$push": {
                                            "oss_info": ossObj_doc
                                        },
                                        # Add create_time and last_modified
                                        "$currentDate": {
                                            "create_time": True,
                                            "last_modified": True
                                        }
                                    }

                                    mongodb.update(MongoDB.Collections.OSS_STATS_COLL,
                                                   oss_upsert_query, oss_upsert_doc,
                                                   upsert=True)

                    # ============================ INSERT/UPDATE File OPs ================================
                    fopObj = provenObj.FileOpObj_lst
                    for fopData in fopObj:
                        # Skip the file operation data come from Epilog when jobs get finished
                        if fopData.target_fid and fopData.target_fid in provenObj.ignore_file_fids:
                            continue

                        # Get FileOp in dictionary format
                        fopData_doc = fopData.to_dict()
                        # ignore the record if the FID for target file does not exist
                        if not fopData_doc.get("target_fid", None):
                            continue
                        # ----------------- Update File Op record if exists ------------------
                        # If a document with this uid and target_fid exists, just add the
                        # new file operation that occurred on this file
                        fileop_updt_q = {'uid': uid, 'target_fid': fopData_doc["target_fid"]}

                        # Update File OP doc
                        fileop_updt_doc = {
                            # Add the current file operation into the list of operations
                            "$push": {
                                "file_ops": {
                                    key: fopData_doc[key] for key in ["op_type", "open_mode", "timestamp", "ext_attr"]
                                }
                            },
                            "$currentDate": {
                                "last_modified": True,
                            }
                        }

                        # Update the target_path and parent_path if the path is already available
                        fileop_updt_set = {}
                        if fopData_doc['target_path'] != "File_Not_Exist":
                            fileop_updt_set['target_path'] = fopData_doc['target_path']

                        if fopData_doc['parent_path'] != "File_Not_Exist":
                            fileop_updt_set['parent_path'] = fopData_doc['parent_path']

                        if fileop_updt_set:
                            fileop_updt_doc["$set"] = fileop_updt_set

                        result = mongodb.update(MongoDB.Collections.FILE_OP_COLL,
                                                fileop_updt_q, fileop_updt_doc,
                                                upsert=False)

                        # ----------------- Insert all new File Ops Documents ------------------
                        # If the document didn't exist to be updated, then insert it
                        # as a new document into the database
                        if result == MongoDB.Result.NO_CHANGE:
                            fopData_doc["file_ops"] = [
                                {
                                    key: fopData_doc.pop(key) for key in
                                        ["op_type", "open_mode", "timestamp", "ext_attr"]
                                }
                            ]
                            fopData_doc["create_time"] = datetime.now()
                            # Do not include the RecID
                            fopData_doc.pop("recID")

                            mongodb.insert(MongoDB.Collections.FILE_OP_COLL, fopData_doc)

                    # ============================ INSERT/UPDATE JobInfo===================================
                    # Update/Insert JobInfo
                    jobinfo = provenObj.jobInfo
                    # First check the JobInfo type. The JobInfo should be a subclass of the original JobInfo
                    # Otherwise, it's not "complete". In some rare cases the JobInfo gets created but before
                    # gets a chance to be filled out by the "JobScheduler" class, it comes here to be stored
                    # in database. That should be ignored!
                    if jobinfo and isinstance(jobinfo, UGEJobInfo):  # 'or' isinstance(Slurm...)
                        # ------------------------- Job Script -----------------------------
                        jobinfo_json = jobinfo.to_dict()
                        # Insert Job Script content in a different collection
                        job_script = jobinfo_json.pop('job_script')
                        # Insert job_script only if exists:
                        if job_script and job_script != "-1":
                            try:
                                data_doc = {'jobid': jobinfo_json['jobid'],
                                            'cluster': jobinfo_json['cluster'],
                                            'job_script': job_script}
                                # Insert only one unique document per job script
                                mongodb.insert(MongoDB.Collections.JOB_SCRIPT_COLL, data_doc)

                            # Ignore the duplicate key error for multiple job_script insertion
                            except DuplicateKeyError:
                                pass

                        # -------------------------- Job Info ------------------------------
                        if isinstance(jobinfo, UGEJobInfo):
                            # do not store the undef_cnt attribute
                            jobinfo_json.pop('undef_cnt')

                        # Insert/Update one job per document, Ignore JobInfos after the jobs is finished
                        update_query = {'uid': uid, 'status': {"$ne": "FINISHED"}}
                        # # Ignore updating the DB for following status but still allowed to insert as a new doc
                        # if jobinfo.status in [JobInfo.Status.UNDEF, JobInfo.Status.OTHERS, JobInfo.Status.NONE]:
                        #     update_query.update({'$and' : [
                        #         {'status': {"$ne": "FINISHED"}},
                        #         {'status': {"$ne": "RUNNING"}}
                        #     ]})
                        #
                        # # Do not allow to insert anything after job gets finished
                        # else:
                        #     update_query.update({'status': {"$ne": "FINISHED"}})

                        try:
                            # Make the update request
                            jobinfo_update = {
                                "$set": jobinfo_json,
                                # Add create_time upon first insertion
                                "$setOnInsert": {"create_time": datetime.now()},
                                # Add modified_time upon each update
                                "$currentDate": {"modified_time": True}
                            }

                            mongodb.update(MongoDB.Collections.JOB_INFO_COLL,
                                           update_query,
                                           jobinfo_update)

                        # Ignore the duplicate key error after the job is finished
                        except DuplicateKeyError:
                            pass

                        # -------------------------- Update MDS Job Status ----------------------------
                        # Update the job status on MDS data to help reduce JOIN with JobInfo collection
                        # Also update the username in case it was not available before
                        mds_update_q = {'uid': uid}
                        mds_update_doc = {
                            "$set": {
                                "status": jobinfo_json["status"],
                                "username": jobinfo_json["username"]
                            },
                            "$currentDate": {
                                "last_modified": True
                            }
                        }
                        mongodb.update(MongoDB.Collections.MDS_STATS_COLL,
                                       mds_update_q, mds_update_doc,
                                       upsert=False)

                        # -------------------------- Update OSS Job Status ----------------------------
                        # Update the job status on OSS data to help reduce JOIN with JobInfo collection
                        # Also update the username in case it was not available before
                        oss_update_q = {'uid': uid}
                        oss_update_doc = {
                            "$set": {
                                "status": jobinfo_json["status"],
                                "username": jobinfo_json["username"]
                            },
                            "$currentDate": {
                                "last_modified": True
                            }
                        }
                        mongodb.update(MongoDB.Collections.OSS_STATS_COLL,
                                       oss_update_q, oss_update_doc,
                                       upsert=False)


        except DBManagerException as dbExp:
            log(Mode.DB_OPERATION, dbExp.getMessage())

        finally:
            # Close the MongoClient
            if mongodb:
                mongodb.close()



class InfluxOPs:
    """
        This class provides methods to store time series data
        into InfluxDB database
    """
    #
    # Dump to InfluxDB database
    #
    @staticmethod
    def dump2InfluxDB(provenData: Dict[str, 'aggr.ProvenanceObj'], serverStats_Q: Queue):
        """
        This method will be called in a different process cuncurrent with other database's operations
        and Dump time series data into InfluxDB for:
            - MDS
            - OSS
        :param provenData: Dict of ProvenanceObj
        :param serverStats_Q: A Queue of Server Status Info
        :return:
        """
        influxdb = None
        try:
            # Create and instance of InfluxDB Connection
            influxdb = InfluxDB()
            # Keep data points in a List
            data_points = []
            # Extract MDS and OSS Data from ProvenanceObjectTable
            for uid, provenObj in provenData.items():
                #================= MDS =================
                # Get a table of latest MDS_MDT data objects
                mdsObj_tbl = provenObj.get_MDS_table()
                # Update/Insert last collected MDS data
                if mdsObj_tbl:
                    # Update/Insert per MDS and MDT
                    for mds_host, mds in mdsObj_tbl.items():
                        for mdt_target, mdsObj in mds.items():
                            # Calculate the total mds operations per job
                            total_ops = 0
                            for attr in ["open", "close", "crossdir_rename", "getattr", "link", "mkdir", "mknod",
                                         "rename", "rmdir", "samedir_rename", "setattr", "unlink", "statfs"]:
                                total_ops += int(getattr(mdsObj, attr))
                            # Tags will be indexed and make the query faster
                            tags = {
                                "host": mds_host,
                                "target": mdt_target,
                                "clustre": mdsObj.cluster
                            }
                            # Data Fields
                            fields = {
                                "jobid": (mdsObj.jobid if mdsObj.jobid else mdsObj.procid),
                                "taskid": mdsObj.taskid,
                                "open": mdsObj.open,
                                "close": mdsObj.close,
                                "crossdir_rename": mdsObj.crossdir_rename,
                                "getattr": mdsObj.getattr,
                                "link": mdsObj.link,
                                "mkdir": mdsObj.mkdir,
                                "mknod": mdsObj.mknod,
                                "rename": mdsObj.rename,
                                "rmdir": mdsObj.rmdir,
                                "samedir_rename": mdsObj.samedir_rename,
                                "setattr": mdsObj.setattr,
                                "remove": mdsObj.unlink,
                                "statfs": mdsObj.statfs,
                                "total_ops": total_ops
                            }
                            # Timestamp
                            time = datetime.fromtimestamp(int(mdsObj.snapshot_time))
                            # Create a Data Point
                            dataPoint = InfluxDB.InfluxObject(
                                measurement=InfluxDB.Measurements.MDS_MEAS,
                                tags=tags,
                                fields=fields,
                                time=time,
                                tzone=influxdb.timezone
                            )
                            # Add data point to the list that has to be inserted
                            data_points.append(dataPoint.to_dict())

                # ================= OSS =================
                # Get a table of latest OSS_OST data objects
                ossObj_tbl = provenObj.get_OSS_table()
                # Update/Insert last collected OSS_MDS data
                if ossObj_tbl:
                    # Update/Insert per OSS and OST
                    for oss_host, ost_tble in ossObj_tbl.items():
                        for ost_target, ossObj in ost_tble.items():
                            # Tags will be indexed and make the query faster
                            tags = {
                                "host": oss_host,
                                "target": ost_target,
                                "clustre": ossObj.cluster
                            }
                            # Data Fields
                            fields = {
                                "jobid": (ossObj.jobid if ossObj.jobid else ossObj.procid),
                                "taskid": ossObj.taskid,
                                "read_ops": ossObj.read_bytes,
                                "read_bytes": ossObj.read_bytes_sum,
                                "write_ops": ossObj.write_bytes,
                                "write_bytes": ossObj.write_bytes_sum
                            }
                            #Timestamp
                            time = datetime.fromtimestamp(int(ossObj.snapshot_time))
                            # Create a Data Point
                            dataPoint = InfluxDB.InfluxObject(
                                measurement=InfluxDB.Measurements.OSS_MEAS,
                                tags=tags,
                                fields=fields,
                                time=time,
                                tzone=influxdb.timezone
                            )
                            # Add data point to the list that has to be inserted
                            data_points.append(dataPoint.to_dict())

            #================= Collect Server Stats ======================
            serverStat_set = set()
            # Collect all the Server Stats up to this point and keep then in a unique set
            while not serverStats_Q.empty():
                # Collect all the server stats
                serverStat_set.add(serverStats_Q.get())

            for serverStat in serverStat_set:
                # Extract hostname, timestamp, and cpu load avg
                host, timestamp, loadAvg, memUsage = serverStat.split(';')
                # Convert loadAvg and memUsage strings to array
                loadAvg = eval(loadAvg)
                memUsage = eval(memUsage)
                # Tags will be indexed and make the query faster
                tags = {
                    "hostname": host
                }
                # Data Fields
                fields = {
                    "cpu_load_avg_1min": loadAvg[0],
                    "cpu_load_avg_5min": loadAvg[1],
                    "cpu_load_avg_15min": loadAvg[2],
                    "total_mem": memUsage[0],
                    "used_mem": memUsage[1]
                }
                # Timestamp
                time = datetime.fromtimestamp(float(timestamp))
                # Create a Data Point
                dataPoint = InfluxDB.InfluxObject(
                    measurement=InfluxDB.Measurements.SERVER_STATS_MEAS,
                    tags=tags,
                    fields=fields,
                    time=time,
                    tzone=influxdb.timezone
                )
                # Add data point to the list that has to be inserted
                data_points.append(dataPoint.to_dict())


            # Now insert all collected time series data into InfluxDB
            if data_points:
                influxdb.insert(data_points)

        except DBManagerException as dbExp:
            log(Mode.DB_OPERATION, dbExp.getMessage())

        finally:
            if influxdb:
                # Close InfluxDB Connection
                influxdb.close()

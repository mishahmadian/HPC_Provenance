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
                # only store data for those data that have JOB_INF
                if provenObj.jobInfo:
                    # Update/Insert JobInfo
                    jobinfo = provenObj.jobInfo
                    #============================ INSERT/UPDATE JobInfo===================================
                    # First check the JobInfo type. The JobInfo should be a subclass of the original JobInfo
                    # Otherwise, it's not "complete". In some rare cases the JobInfo gets created but before
                    # gets a chance to be filled out by the "JobScheduler" class, it comes here to be stored
                    # in database. That should be ignored!
                    if jobinfo and isinstance(jobinfo, UGEJobInfo): # 'or' isinstance(Slurm...)
                        #------------------------- Job Script -----------------------------
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
                        # Insert/Update one job per document, Ignore JobInfos after the jobs is finished
                        update_query = {'uid' : uid, 'status': {"$ne": "FINISHED"}}
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
                                "$set" : jobinfo_json,
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
                                update_query = {'uid': uid, 'oss_host': oss_host, 'ost_target': ost_target}

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
            log(Mode.DB_OPERATION, dbExp.getMessage())

        finally:
            # Close the MongoClient
            #print("=== Dumped to MongoDB ===")
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
                                "statfs": mdsObj.statfs
                            }
                            # Timestamp
                            time = datetime.fromtimestamp(int(mdsObj.snapshot_time))
                            # Create a Data Point
                            dataPoint = InfluxDB.InfluxObject(
                                measurement=InfluxDB.Measurements.MDS_MEAS,
                                tags=tags,
                                fields=fields,
                                time=time
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
                                time=time
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
                    time=time
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

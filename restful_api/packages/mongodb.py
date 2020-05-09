# -*- coding: utf-8 -*-
"""
    This module manages the Access to the Provenance MongoDB instance
    and provides all sets of required queries

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from api_config import Config, ConfigReadExcetion
from datetime import datetime, timedelta
from pymongo.errors import PyMongoError
from api_logger import log, Mode
from pymongo import MongoClient
from typing import List, Dict
from enum import Enum, unique
from bson.son import SON

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
            raise DBManagerException(f"(close) {str(mongoExp)}")


    def query_oss_jobs(self, oss_resource, ost_target=None, uid=None, jobstatus=None,
                       sort_param=None, days=None) -> List[Dict]:
        """
            Aggregate the OSS/OST data with their corresponding jobs
        :return:
        """
        # ---------- Creating Aggregation query ------------
        # Filter oss data
        filter_oss = {
            u"$match": {
                u"oss_host": oss_resource
            }
        }
        if ost_target:
            filter_oss[u"$match"].update({u"ost_target" : ost_target})
        if uid:
            filter_oss[u"$match"].update({u"uid": uid})
        if days:
            filter_oss.update({"create_time": {
                "$gte": datetime.now() - timedelta(days=days)
            }})

        # JOIN OSS_STATS with JobInfo collection
        join = {
            u"$lookup": {
                u"from": u"jobinfo",
                u"localField": u"uid",
                u"foreignField": u"uid",
                u"as": u"job_info"
            }
        }

        # Select the Output Results
        projection = {
            u"$project": {
                u"_id": 0.0,
                u"oss_host": 1.0,
                u"ost_target": 1.0,
                u"uid": 1.0,
                u"jobid": 1.0,
                u"read_bytes": 1.0,
                u"read_bytes_max": 1.0,
                u"read_bytes_min": 1.0,
                u"read_bytes_sum": 1.0,
                u"write_bytes": 1.0,
                u"write_bytes_max": 1.0,
                u"write_bytes_min": 1.0,
                u"write_bytes_sum": 1.0,
                u"modified_time": 1.0,
                u"job_info.jobid": 1.0,
                u"job_info.taskid": 1.0,
                u"job_info.status": 1.0,
                u"job_info.username": 1.0,
                u"job_info.start_time": 1.0
            }
        }
        # Filter based on JobInfo
        filter_jobs = {
            u"$match": {
                u"job_info.status": u"RUNNING"
            }
        }
        if jobstatus:
            filter_jobs[u"$match"].update({u"job_info.status": jobstatus})

        # Find the Field that has the highest activity on OSS
        highest_activity = {
            u"$addFields": {
                u"highest_ops": {
                    u"$max": [
                        u"$read_bytes",
                        u"$write_bytes"
                    ]
                }
            }
        }

        # SORT Output Results
        sort = {
            u"$sort": SON([ (u"highest_ops", -1) ])
        }

        if sort_param:
            if sort_param in projection.get("$project").keys():
                sort.update({u"$sort": SON([(u"sort_param", -1)])})

            elif f"job_info.{sort_param}" in projection.get("$project").keys():
                sort.update({u"$sort": SON([(f"job_info.{sort_param}", -1)])})

            else:
                return [{"error" : "The Sort parameter is not applicable"}]

        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [filter_oss, join, projection, filter_jobs, highest_activity, sort]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.OSS_STATS_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse = False)

        return list(cursor)


    def query_mds_jobs(self, mds_resource, mdt_target=None, uid=None, jobstatus=None,
                       sort_param=None, days=None) -> List[Dict]:
        """
            Aggregate the MDS/MDT data with their corresponding jobs
        :return:
        """
        # ---------- Creating Aggregation query ------------
        # Filter MDS data
        filter_mds = {
            u"$match": {
                u"mds_host": mds_resource
            }
        }
        if mdt_target:
            filter_mds[u"$match"].update({u"mdt_target": mdt_target})
        if uid:
            filter_mds[u"$match"].update({u"uid": uid})
        if days:
            filter_mds.update({"create_time": {
                "$gte": datetime.now() - timedelta(days=days)
            }})

        # JOIN MDS_STATS with JobInfo collection
        join = {
            u"$lookup": {
                u"from": u"jobinfo",
                u"localField": u"uid",
                u"foreignField": u"uid",
                u"as": u"job_info"
            }
        }

        # Select the Output Fields
        projection = {
            u"$project": {
                u"_id": 0.0,
                u"mds_host": 1.0,
                u"mdt_target": 1.0,
                u"uid": 1.0,
                u"close": 1.0,
                u"crossdir_rename": 1.0,
                u"getattr": 1.0,
                u"jobid": 1.0,
                u"link": 1.0,
                u"mkdir": 1.0,
                u"mknod": 1.0,
                u"open": 1.0,
                u"rename": 1.0,
                u"rmdir": 1.0,
                u"samedir_rename": 1.0,
                u"setattr": 1.0,
                u"unlink": 1.0,
                u"modified_time": 1.0,
                u"job_info.jobid": 1.0,
                u"job_info.taskid": 1.0,
                u"job_info.status": 1.0,
                u"job_info.username": 1.0,
                u"job_info.start_time": 1.0
            }
        }

        # Filter based on JobInfo
        filter_jobs = {
            u"$match": {
                u"job_info.status": u"RUNNING"
            }
        }
        if jobstatus:
            filter_jobs[u"$match"].update({u"job_info.status": jobstatus})

        # Find the Field that has the highest activity on MDS
        highest_activity = {
            u"$addFields": {
                u"highest_activity": {
                    u"$max": [
                        u"$open",
                        u"$close",
                        u"$crossdir_rename",
                        u"$getattr",
                        u"$link",
                        u"$mkdir",
                        u"$mknod",
                        u"$rename",
                        u"$rmdir",
                        u"$samedir_rename",
                        u"$setattr",
                        u"$unlink"
                    ]
                }
            }
        }

        # SORT Output Results
        sort = {
            u"$sort": SON([(u"highest_activity", -1)])
        }

        if sort_param:
            if sort_param in projection.get("$project").keys():
                sort.update({u"$sort" : SON([(u"sort_param", -1)])})

            elif f"job_info.{sort_param}" in projection.get("$project").keys():
                sort.update({u"$sort": SON([(f"job_info.{sort_param}", -1)])})

            else:
                return [{"error" : "The Sort parameter is not applicable"}]


        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [filter_mds, join, projection, filter_jobs, highest_activity, sort]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.MDS_STATS_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse=False)

        return list(cursor)


    def query_fileop_mds(self, mdt_target , uid=None, jobstatus=None, sort_param=None, days=None) -> List[Dict]:
        """
            Get File Operations for jobs on each MDT
        :return:
        """
        # ---------- Creating Aggregation query ------------
        # Filter fileOPs data
        filter_fileops = {
            u"$match": {
                u"mdtTarget": mdt_target
            }
        }

        if uid:
            filter_fileops[u"$match"].update({u"uid": uid})
        if days:
            filter_fileops.update({"create_time": {
                "$gte": datetime.now() - timedelta(days=days)
            }})

        # JOIN FILE_OP with JobInfo collection
        join = {
        u"$lookup": {
                u"from": u"jobinfo",
                u"localField": u"uid",
                u"foreignField": u"uid",
                u"as": u"job_info"
            }
        }

        # Select the Output Fields
        projection = {
            u"$project": {
                u"_id": 0.0,
                u"jobid": 1.0,
                u"mdtTarget": 1.0,
                u"nid": 1.0,
                u"op_type": 1.0,
                u"open_mode": 1.0,
                u"parent_path": 1.0,
                u"target_file": 1.0,
                u"target_path": 1.0,
                u"taskid": 1.0,
                u"timestamp": 1.0,
                u"uid": 1.0,
                u"job_info.jobid": 1.0,
                u"job_info.taskid": 1.0,
                u"job_info.status": 1.0,
                u"job_info.username": 1.0,
                u"job_info.start_time": 1.0
            }
        }

        # Filter based on JobInfo
        filter_jobs = {
            u"$match": {
                u"job_info.status": u"RUNNING"
            }
        }
        if jobstatus:
            filter_jobs[u"$match"].update({u"job_info.status": jobstatus})

        # SORT Output Results
        sort = {
            u"$sort": SON([(u"timestamp", -1)])
        }

        if sort_param:
            if sort_param in projection.get("$project").keys():
                sort.update({u"$sort" : SON([(u"sort_param", -1)])})

            elif f"job_info.{sort_param}" in projection.get("$project").keys():
                sort.update({u"$sort": SON([(f"job_info.{sort_param}", -1)])})

            else:
                return [{"error" : "The Sort parameter is not applicable"}]


        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [filter_fileops, join, projection, filter_jobs, sort]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.FILE_OP_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse=False)

        return list(cursor)


    def query_jobinfo_all(self, jobstatus=None, sort_param=None, days=None) -> List[Dict]:
        """
            Get File Operations for jobs on each MDT
        :return:
        """
        # Filter JobInfo
        query = {"status": "RUNNING"}
        if jobstatus:
            query.update({"status": jobstatus})
        if days:
            query.update({"create_time": {
                "$gte": datetime.now() - timedelta(days=days)
            }})

        # Select required fields:
        projection = {
            "_id" : 0.0,
            "end_time" : 1.0,
            "modified_time" : 1.0,
            "num_cpu" : 1.0,
            "h_vmem" : 1.0,
            "parallelEnv" : 1.0,
            "project" : 1.0,
            "queue" : 1.0,
            "start_time" : 1.0,
            "status" : 1.0,
            "submit_time" : 1.0,
            "taskid" : 1.0,
            "username" : 1.0,
            "jobName" : 1.0,
            "jobid" : 1.0
        }

        # Sort Items
        sort = [(u"jobid", -1), (u"taskid", 1)]
        if sort_param:
            sort = [(sort_param, -1)]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.JOB_INFO_COLL.value]
        # Perform the Query
        cursor = coll.find(query, projection=projection, sort=sort)
        return list(cursor)


    def query_jobinfo_detail(self, uid) -> List[Dict]:
        """
            Get File Operations for jobs on each MDT
        :return:
        """
        # Filter JobInfo
        jobInfo_query = {
            u"$match": {
                u"uid": uid,
            }
        }

        join_mds_fileop = {
            u"$lookup": {
                u"from": u"mds_stats",
                u"let": { u"j_uid": u"$uid" },
                u"pipeline": [
                    { u"$match": {
                            u"$expr": {
                                u"$eq": [ u"$uid", u"$$j_uid" ]
                            }
                        }
                    },
                    { u"$lookup": {
                            u"from": u"file_op",
                            u"let": {
                                u"mds_uid": u"$uid",
                                u"mdt": u"$mdt_target"
                            },
                            u"pipeline": [
                                { u"$match": {
                                        u"$expr": {
                                            u"$and": [
                                                { u"$eq": [ u"$uid", u"$$mds_uid" ] },
                                                { u"$eq": [ u"$mdtTarget", u"$$mdt" ] }
                                            ]
                                        }
                                    }
                                },
                                { u"$project": {
                                        u"_id": 0.0,
                                        u"mdtTarget": 1.0,
                                        u"nid": 1.0,
                                        u"op_type": 1.0,
                                        u"open_mode": 1.0,
                                        u"parent_path": 1.0,
                                        u"target_file": 1.0,
                                        u"target_path": 1.0,
                                        u"timestamp": 1.0
                                    }
                                },
                                { u"$sort": {
                                        u"timestamp": -1.0
                                    }
                                }
                            ],
                            u"as": u"file_op"
                        }
                    },
                    { u"$project": {
                            u"_id": 0.0,
                            u"mds_host": 1.0,
                            u"mdt_target": 1.0,
                            u"close": 1.0,
                            u"crossdir_rename": 1.0,
                            u"getattr": 1.0,
                            u"link": 1.0,
                            u"mkdir": 1.0,
                            u"mknod": 1.0,
                            u"open": 1.0,
                            u"rename": 1.0,
                            u"rmdir": 1.0,
                            u"samedir_rename": 1.0,
                            u"setattr": 1.0,
                            u"unlink": 1.0,
                            u"file_op": 1.0
                        }
                    },
                    { u"$sort": {
                            u"mds_host": 1.0,
                            u"mdt_target": 1.0
                        }
                    }
                ],
                u"as": u"mds_data"
            }
        }

        join_oss = {
            u"$lookup": {
                u"from": u"oss_stats",
                u"let": {
                    u"j_uid": u"$uid"
                },
                u"pipeline": [
                    {u"$match": {
                            u"$expr": {
                                u"$eq": [ u"$uid", u"$$j_uid" ]
                            }
                        }
                    },
                    { u"$project": {
                        u"_id": 0.0,
                        u"oss_host": 1.0,
                        u"ost_target": 1.0,
                        u"read_bytes": 1.0,
                        u"read_bytes_max": 1.0,
                        u"read_bytes_min": 1.0,
                        u"read_bytes_sum": 1.0,
                        u"write_bytes": 1.0,
                        u"write_bytes_max": 1.0,
                        u"write_bytes_min": 1.0,
                        u"write_bytes_sum": 1.0
                        }
                    },
                    { u"$sort": {
                            u"oss_host": 1.0,
                            u"ost_target": 1.0
                        }
                    }
                ],
                u"as": u"oss_data"
            }
        }

        projection = {
            u"$project": {
                u"_id": 0.0,
                u"uid": 1.0,
                u"command": 1.0,
                u"end_time": 1.0,
                u"exec_host": 1.0,
                u"failed_no": 1.0,
                u"h_rt": 1.0,
                u"h_vmem": 1.0,
                u"modified_time": 1.0,
                u"num_cpu": 1.0,
                u"parallelEnv": 1.0,
                u"project": 1.0,
                u"q_del": 1.0,
                u"queue": 1.0,
                u"s_rt": 1.0,
                u"start_time": 1.0,
                u"status": 1.0,
                u"submit_time": 1.0,
                u"taskid": 1.0,
                u"jobid": 1.0,
                u"jobName": 1.0,
                u"pwd": 1.0,
                u"username": 1.0,
                u"oss_data": 1.0,
                u"mds_data": 1.0
            }
        }

        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [jobInfo_query, join_mds_fileop, join_oss, projection]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.JOB_INFO_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse=False)

        return list(cursor)

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
    def __init__(self, message: str):
        super(DBManagerException, self).__init__(message)
        self.message = message

    def getMessage(self):
        return "\n [Error] _DBManager_: {} \n".format(self.message)

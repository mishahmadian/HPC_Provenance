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
                       sort_param=None, days=None, username=None, cluster=None) -> List[Dict]:
        """
            Aggregate the OSS/OST data with their corresponding jobs
        :return:
        """
        # ---------- Creating Aggregation query ------------
        # Filter oss data
        filter_oss = {
            u"$match": {
                u"oss_info.oss_host": oss_resource,
                u"status": u"RUNNING"
            }
        }

        if uid:
            filter_oss[u"$match"].update({u"uid": uid})
        if cluster:
            filter_oss[u"$match"].update({u"cluster": cluster})
        if ost_target:
            filter_oss[u"$match"].update({u"oss_info.ost_target": ost_target})
        if jobstatus:
            filter_oss[u"$match"].update({u"status": jobstatus})
        if username:
            filter_oss[u"$match"].update({u"username": username})
        if days:
            filter_oss[u"$match"].update({"create_time": {
                "$gte": datetime.now() - timedelta(days=days)
            }})

        ossinfo_expand = {
            u"$unwind": {
                u"path": u"$oss_info",
                u"preserveNullAndEmptyArrays": False
            }
        }

        filter_ossinfo = {
            u"$match": {
                u"oss_info.oss_host": oss_resource
            }
        }

        if ost_target:
            filter_ossinfo[u"$match"].update({u"oss_info.ost_target" : ost_target})

        # Select the Output Results
        projection = {
            u"$project": {
                u"_id": 0.0,
                u"uid": 1.0,
                u"jobid": 1.0,
                u"taskid": 1.0,
                u"cluster": 1.0,
                u"status": 1.0,
                u"username": 1.0,
                u"oss_info.oss_host": 1.0,
                u"oss_info.ost_target": 1.0,
                u"oss_info.read_bytes": 1.0,
                u"oss_info.read_bytes_max": 1.0,
                u"oss_info.read_bytes_min": 1.0,
                u"oss_info.read_bytes_sum": 1.0,
                u"oss_info.write_bytes": 1.0,
                u"oss_info.write_bytes_max": 1.0,
                u"oss_info.write_bytes_min": 1.0,
                u"oss_info.write_bytes_sum": 1.0,
                u"oss_info.modified_time": 1.0
            }
        }

        # Find the Field that has the highest activity on OSS
        highest_activity = {
            u"$addFields": {
                u"highest_ops": {
                    u"$max": [
                        u"$oss_info.read_bytes",
                        u"$oss_info.write_bytes"
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
                sort.update({u"$sort": SON([(sort_param, -1)])})

            elif f"oss_info.{sort_param}" in projection.get("$project").keys():
                sort.update({u"$sort": SON([(f"oss_info.{sort_param}", -1)])})

            else:
                return [{"error" : "The Sort parameter is not applicable"}]

        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [filter_oss, ossinfo_expand, filter_ossinfo, projection, highest_activity, sort]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.OSS_STATS_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse = False)

        return list(cursor)


    def query_mds_jobs(self, mds_resource, mdt_target=None, uid=None, jobstatus=None,
                       sort_param=None, days=None, username=None, cluster=None) -> List[Dict]:
        """
            Aggregate the MDS/MDT data with their corresponding jobs
        :return:
        """
        # ---------- Creating Aggregation query ------------
        # Filter MDS data
        filter_mds = {
            u"$match": {
                u"mds_info.mds_host": mds_resource,
                u"status": u"RUNNING"
            }
        }

        if uid:
            filter_mds[u"$match"].update({u"uid": uid})
        if cluster:
            filter_mds[u"$match"].update({u"cluster": cluster})
        if mdt_target:
            filter_mds[u"$match"].update({u"mds_info.mdt_target": mdt_target})
        if jobstatus:
            filter_mds[u"$match"].update({u"status": jobstatus})
        if username:
            filter_mds[u"$match"].update({u"username": username})
        if days:
            filter_mds[u"$match"].update({"create_time": {
                "$gte": datetime.now() - timedelta(days=days)
            }})

        mdsinfo_expand = {
            u"$unwind": {
                u"path": u"$mds_info",
                u"preserveNullAndEmptyArrays": False
            }
        }

        filter_mdsinfo = {
            u"$match": {
                u"mds_info.mds_host": mds_resource
            }
        }

        if mdt_target:
            filter_mdsinfo[u"$match"].update({u"mds_info.mdt_target" : mdt_target})

        # Select the Output Fields
        projection = {
            u"$project": {
                u"_id": 0.0,
                u"uid": 1.0,
                u"jobid": 1.0,
                u"taskid": 1.0,
                u"cluster": 1.0,
                u"status": 1.0,
                u"username": 1.0,
                u"mds_info.mds_host": 1.0,
                u"mds_info.mdt_target": 1.0,
                u"mds_info.close": 1.0,
                u"mds_info.crossdir_rename": 1.0,
                u"mds_info.getattr": 1.0,
                u"mds_info.link": 1.0,
                u"mds_info.mkdir": 1.0,
                u"mds_info.mknod": 1.0,
                u"mds_info.open": 1.0,
                u"mds_info.rename": 1.0,
                u"mds_info.rmdir": 1.0,
                u"mds_info.samedir_rename": 1.0,
                u"mds_info.setattr": 1.0,
                u"mds_info.unlink": 1.0,
                u"mds_info.statfs": 1.0,
                u"mds_info.modified_time": 1.0,
            }
        }

        # Find the Field that has the highest activity on MDS
        highest_activity = {
            u"$addFields": {
                u"highest_activity": {
                    u"$max": [
                        u"$mds_info.open",
                        u"$mds_info.close",
                        u"$mds_info.crossdir_rename",
                        u"$mds_info.getattr",
                        u"$mds_info.link",
                        u"$mds_info.mkdir",
                        u"$mds_info.mknod",
                        u"$mds_info.rename",
                        u"$mds_info.rmdir",
                        u"$mds_info.samedir_rename",
                        u"$mds_info.setattr",
                        u"$mds_info.unlink",
                        u"$mds_info.statfs"
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
                sort.update({u"$sort" : SON([(sort_param, -1)])})

            elif f"oss_info.{sort_param}" in projection.get("$project").keys():
                sort.update({u"$sort": SON([(f"oss_info.{sort_param}", -1)])})

            else:
                return [{"error" : "The Sort parameter is not applicable"}]


        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [filter_mds, mdsinfo_expand, filter_mdsinfo, projection, highest_activity, sort]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.MDS_STATS_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse=False)

        return list(cursor)


    def query_fileop_mds(self, mdt_target , uid=None, jobstatus=None, sort_param=None,
                         days=None, username=None, cluster=None) -> List[Dict]:
        """
            Get File Operations for jobs on each MDT
        :return:
        """
        # ---------- Creating Aggregation query ------------
        # Filter fileOPs data
        filter_jobs = {
            u"$match": {
                u"status": u"RUNNING"
            }
        }

        if uid:
            filter_jobs[u"$match"].update({u"uid": uid})
        if jobstatus:
            filter_jobs[u"$match"].update({u"status": jobstatus})
        if username:
            filter_jobs[u"$match"].update({u"username": username})
        if cluster:
            filter_jobs[u"$match"].update({u"cluster": cluster})

        # JOIN FILE_OP with JobInfo collection
        join_fileop = {
            u"$lookup": {
                u"from": u"file_op",
                u"let": {
                    u"j_uid": u"$uid",
                    u"mdt": mdt_target
                },
                u"pipeline": [
                    {
                        u"$match": {
                            u"$expr": {
                                u"$and": [
                                    { u"$eq": [ u"$uid", u"$$j_uid" ] },
                                    { u"$eq": [ u"$mdtTarget", u"$$mdt" ] }
                                ]
                            }
                        }
                    },
                    {
                        u"$project": {
                            u"_id": 0.0,
                            u"mdtTarget": 1.0,
                            u"nid": 1.0,
                            u"target_path": 1.0,
                            u"target_file": 1.0,
                            u"parent_path": 1.0,
                            u"file_ops": 1.0,
                            u"create_time": 1.0
                        }
                    }
                ],
                u"as": u"fileop_info"
            }
        }

        if days:
            join_fileop[u"$lookup"][u"pipeline"][0][u"$match"][u"$expr"][u"$and"].append(
                { u"$gte": ["$create_time", datetime.now() - timedelta(days=days)] }
            )

        fileop_info_expand = {
            u"$unwind": {
                u"path": u"$fileop_info",
                u"preserveNullAndEmptyArrays": False
            }
        }

        fileop_expand = {
            u"$unwind": {
                u"path": u"$fileop_info.file_ops",
                u"preserveNullAndEmptyArrays": False
            }
        }

        # Select the Output Fields
        projection = {
            u"$project": {
                u"_id": 0.0,
                u"uid": 1.0,
                u"jobid": 1.0,
                u"taskid": 1.0,
                u"cluster": 1.0,
                u"status": 1.0,
                u"username": 1.0,
                u"fileop_info": 1.0,
            }
        }

        # SORT Output Results
        sort = {
            u"$sort": SON([ (u"fileop_info.file_ops.timestamp", -1) ])
        }

        if sort_param:
            if sort_param in projection.get("$project").keys():
                sort.update({u"$sort" : SON([(sort_param, -1)])})

            else:
                return [{"error" : "The Sort parameter is not applicable"}]


        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [filter_jobs, join_fileop, fileop_info_expand, fileop_expand, projection, sort]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.JOB_INFO_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse=True)

        return list(cursor)


    def query_jobinfo_all(self, jobstatus=None, sort_param=None, days=None, username=None, cluster=None) -> List[Dict]:
        """
            Get File Operations for jobs on each MDT
        :return:
        """
        # Filter JobInfo
        query = {"status": "RUNNING"}
        if jobstatus:
            query.update({"status": jobstatus})
        if username:
            query.update({"username": username})
        if cluster:
            query.update({"cluster": cluster})
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
            "jobid" : 1.0,
            "cluster": 1.0
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

        join_mds = {
            u"$lookup": {
                u"from": u"mds_stats",
                u"let": {
                    u"j_uid": u"$uid"
                },
                u"pipeline": [
                    {
                        u"$match": {
                            u"$expr": {
                                u"$eq": [ u"$uid", u"$$j_uid" ]
                            }
                        }
                    },
                    {
                        u"$project": {
                            u"_id": 0.0,
                            u"mds_info.mds_host": 1.0,
                            u"mds_info.mdt_target": 1.0,
                            u"mds_info.close": 1.0,
                            u"mds_info.crossdir_rename": 1.0,
                            u"mds_info.getattr": 1.0,
                            u"mds_info.link": 1.0,
                            u"mds_info.mkdir": 1.0,
                            u"mds_info.mknod": 1.0,
                            u"mds_info.open": 1.0,
                            u"mds_info.rename": 1.0,
                            u"mds_info.rmdir": 1.0,
                            u"mds_info.samedir_rename": 1.0,
                            u"mds_info.setattr": 1.0,
                            u"mds_info.unlink": 1.0,
                            u"mds_info.statfs": 1.0,
                            u"mds_info.modified_time": 1.0,
                        }
                    },
                    {
                        u"$unwind": {
                            u"path": u"$mds_info",
                            u"preserveNullAndEmptyArrays": False
                        }
                    },
                    {
                        u"$sort": {
                            u"mds_info.mds_host": 1.0,
                            u"mds_info.mdt_target": 1.0
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
                    {
                        u"$match": {
                            u"$expr": {
                                u"$eq": [
                                    u"$uid",
                                    u"$$j_uid"
                                ]
                            }
                        }
                    },
                    {
                        u"$project": {
                            u"_id": 0.0,
                            u"oss_info.oss_host": 1.0,
                            u"oss_info.ost_target": 1.0,
                            u"oss_info.read_bytes": 1.0,
                            u"oss_info.read_bytes_max": 1.0,
                            u"oss_info.read_bytes_min": 1.0,
                            u"oss_info.read_bytes_sum": 1.0,
                            u"oss_info.write_bytes": 1.0,
                            u"oss_info.write_bytes_max": 1.0,
                            u"oss_info.write_bytes_min": 1.0,
                            u"oss_info.write_bytes_sum": 1.0,
                            u"oss_info.modified_time": 1.0,
                        }
                    },
                    {
                        u"$unwind": {
                            u"path": u"$oss_info",
                            u"preserveNullAndEmptyArrays": False
                        }
                    },
                    {
                        u"$sort": {
                            u"oss_info.oss_host": 1.0,
                            u"oss_info.ost_target": 1.0
                        }
                    }
                ],
                u"as": u"oss_data"
            }
        }

        join_fileop = {
            u"$lookup": {
                u"from": u"file_op",
                u"let": {
                    u"j_uid": u"$uid"
                },
                u"pipeline": [
                    {
                        u"$match": {
                            u"$expr": {
                                u"$eq": [
                                    u"$uid",
                                    u"$$j_uid"
                                ]
                            }
                        }
                    },
                    {
                        u"$project": {
                            u"_id": 0.0,
                            u"mdtTarget": 1.0,
                            u"nid": 1.0,
                            u"target_path": 1.0,
                            u"target_file": 1.0,
                            u"parent_path": 1.0,
                            u"file_ops": 1.0,
                            #u"create_time": 1.0
                        }
                    },
                    {
                        u"$unwind": {
                            u"path": u"$file_ops",
                            u"preserveNullAndEmptyArrays": False
                        }
                    },
                    {
                        u"$sort": {
                            u"file_ops.timestamp": -1.0
                        }
                    }
                ],
                u"as": u"fileop_data"
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
                u"cluster": 1.0,
                u"pwd": 1.0,
                u"username": 1.0,
                u"mds_data": 1.0,
                u"oss_data": 1.0,
                u"fileop_data": 1.0
            }
        }

        # ---------- Generating Aggregation Pipeline ------------
        pipeline = [jobInfo_query, join_mds, join_oss, join_fileop, projection]

        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.JOB_INFO_COLL.value]
        # Perform the Query
        cursor = coll.aggregate(pipeline, allowDiskUse=True)

        return list(cursor)


    def query_jobscript(self, cluster, jobid) -> Dict:
        """
            Get the Job Submission script of the Job
        :return:
        """
        # Selects the collection of this database
        coll = self._mongoDB[self.Collections.JOB_SCRIPT_COLL.value]
        # Perform the Query
        ret_data = coll.find_one({"cluster": cluster, "jobid": jobid})
        return ret_data

    #
    # MongoDB Collection Enum
    #
    @unique
    class Collections(Enum):
        """
            The available collections for this MondoDB are defined in this Enum class
        """
        JOB_INFO_COLL = 'jobinfo'
        JOB_SCRIPT_COLL = 'jobscript'
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

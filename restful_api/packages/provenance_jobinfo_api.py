# -*- coding: utf-8 -*-
"""
    The API Class for JobInfo query requests and responses

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from mongodb import MongoDB, DBManagerException
from flask_restful import Resource, reqparse
from api_logger import log, Mode

class JobInfo(Resource):
    def __init__(self):
        super(JobInfo, self).__init__()

    def get(self, uid=None):
        # Get and parse all the arguments
        req_data = self._get_args()

        db = None
        try:
            db = MongoDB()

            if uid:
                # If uid was passed, then get all the detail information for this job
                results = db.query_jobinfo_detail(uid)
            else:
                # Otherwise, list all the recorded jobs
                results = db.query_jobinfo_all(req_data['js'], req_data['sort'], req_data['days'], req_data['user'])

            # Convert Datetime object to epoch
            for rec in results:
                rec["modified_time"] = rec["modified_time"].timestamp()

                for mdsdata in rec["mds_data"]:
                    mdsdata["mds_info"]["modified_time"] = mdsdata["mds_info"]["modified_time"].timestamp()

                for ossData in rec["oss_data"]:
                    ossData["oss_info"]["modified_time"] = ossData["oss_info"]["modified_time"].timestamp()


            if results:
                return {"result": results}, 200
            else:
                return self._error("No result was found", code=404)


        except DBManagerException as dbExp:
            log(Mode.JOB_INFO_API, dbExp.getMessage())
            return self._error("Error in Database Query")

        finally:
            if db: db.close()

    @staticmethod
    def _get_args():
        """
            Parse all the key/value arguments for this HTTP GET
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('uid', type=str, required=False)
        parser.add_argument('js', type=str, required=False)
        parser.add_argument('user', type=str, required=False)
        parser.add_argument('sort', type=str, required=False)
        parser.add_argument('days', type=int, required=False)
        return parser.parse_args()

    @staticmethod
    def _error(message, code=400):
        """
            Generate an error message and return a dictionary along with the error code
        :param message: String
        :param code: 404, 400, 200, 201
        :return: (dict, int)
        """
        return {
                   "message": message,
                   "code": code
               }, code
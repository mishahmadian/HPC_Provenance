# -*- coding: utf-8 -*-
"""
    The API Class for JobInfo query requests and responses

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from mongodb import MongoDB, DBManagerException
from flask_restful import Resource, reqparse
from api_logger import log, Mode

class JobScript(Resource):
    def __init__(self):
        super(JobScript, self).__init__()

    def get(self, cluster, jobid):

        db = None
        try:
            db = MongoDB()
            result = db.query_jobscript(cluster, jobid)

            if result:
                result.pop("_id")
                return {"result": result}, 200
            else:
                return self._error("No result was found", code=404)

        except DBManagerException as dbExp:
            log(Mode.JOB_SCRIPT_API, dbExp.getMessage())
            return self._error("Error in Database Query")

        finally:
            if db: db.close()

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
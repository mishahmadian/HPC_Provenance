# -*- coding: utf-8 -*-
"""
    The API Class for MDS query requests and responses

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from api_config import Config, ConfigReadExcetion
from mongodb import MongoDB, DBManagerException
from flask_restful import Resource, reqparse
from api_logger import log, Mode
import json

class MDSapi(Resource):
    def __init__(self):
        super(MDSapi, self).__init__()
        config = Config()
        try:
            lustre_schema = config.getSchema()
            # Report if lustre schema was empty
            if not lustre_schema:
                log(Mode.MDS_API, "Lustre Schema was not found")

            # Get the lustre schema in dictionary format
            schema_dict = json.loads(lustre_schema)
            # Get the OSS part of the schema
            self.mds_schema = schema_dict.get('mds', None)

        except ConfigReadExcetion as confExp:
            log(Mode.MDS_API, confExp.getMessage())

    def get(self, resource, target=None):
        """
        The GET METHOD of the RESTful API

        :param resource: The OSS name (Required)
        :param target: The OST name (optional)
        :return: JSON
        """
        # Make sure the 'resource' is in the OSS schema
        if resource not in self.mds_schema.keys():
            return self._error(f"the MDS ({resource}) does not exist")

        # Check target to be in the OST list
        if target and target not in self.mds_schema.get(resource, None):
            return self._error(f"the MDT ({target}) does not belong to ({resource})")

        # Get and parse all the arguments
        req_data = self._get_args()

        db = None
        try:
            db = MongoDB()
            # Get the MDS/Jobs/ and Files that are running on MDS
            if all([target, req_data['files'], req_data['files'] == "y"]):
                result = db.query_fileop_mds(target, req_data['uid'], req_data['js'],
                                             req_data['sort'], req_data['days'], req_data['user'])
            # A target has to be selected
            elif target and req_data['files']:
                return self._error("Cannot list all the files without selecting a terget MDT")
            else:
            # Otherwise, get the MDS and JobInfo Data
                result = db.query_mds_jobs(resource, target, req_data['uid'], req_data['js'],
                                           req_data['sort'], req_data['days'], req_data['user'])

            # Convert Datetime object to epoch
            for rec in result:
                # Convert DateTime data to a serializable format
                if  rec.get("mds_info", None) and rec["mds_info"].get("modified_time", None):
                    rec["mds_info"]["modified_time"] = rec["mds_info"]["modified_time"].timestamp()

                elif rec.get("fileop_info", None) and rec["fileop_info"].get("create_time", None):
                    rec["fileop_info"]["create_time"] = rec["fileop_info"]["create_time"].timestamp()

            if result:
                return {"result": result}, 200
            else:
                return self._error("No result was found", code=404)

        except DBManagerException as dbExp:
            log(Mode.MDS_API, dbExp.getMessage())
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
        parser.add_argument('files', type=str, required=False)
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
# -*- coding: utf-8 -*-
"""
    This Module provides a RESTful API interface for external communication with Provenance Database
    The module will be running on an API Server and should have a separate Service.

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from api_config import Config, ConfigReadExcetion
from provenance_jobinfo_api import JobInfo
from provenance_oss_api import OSSapi
from provenance_mds_api import MDSapi
from gevent.pywsgi import WSGIServer
from flask import Flask, jsonify
from flask_restful import Api
import json


# Create an API app
api_app = Flask(__name__)
# To allow flask propagating exception even if debug is set to false on app
api_app.config['PROPAGATE_EXCEPTIONS'] = False
# Create RESTful API
rest_api = Api(api_app)
# Get Configuration object
config = Config()

@api_app.route('/provenance/schema')
def schema():
    """
    Get the Schema of the lustre servers
    :return:
    """
    try:
        lustre_schema = config.getSchema().strip('][').replace('\n', ' ')
    except ConfigReadExcetion:
        lustre_schema = None

    if not lustre_schema:
        return jsonify(_Error("couldn't find the Lustre Servers Schema"))
    else:
        lustre_schema = json.loads(lustre_schema)

    return jsonify({'schema' : lustre_schema})


def _Error(message, code=400):
    """
    Create an Error framework
    :param message:
    :param code:
    :return:
    """
    return {
        "error" : message,
        "code" : code
    }


if __name__ == '__main__':
    # RestAPI for OSS data and their JobInfo
    rest_api.add_resource(OSSapi,
                          "/provenance/oss/<string:resource>",
                          "/provenance/oss/<string:resource>/<string:target>")
    # RestAPI for MDS data and their JobInfo
    rest_api.add_resource(MDSapi,
                          "/provenance/mds/<string:resource>",
                          "/provenance/mds/<string:resource>/<string:target>")
    # RestAPI for JobInfo data
    rest_api.add_resource(JobInfo,
                          "/provenance/jobinfo",
                          "/provenance/jobinfo/<string:uid>")

    # Setup the API Server
    #api_app.run(port=5000, host="0.0.0.0", debug=True)
    api_server = WSGIServer(('', config.getApiPort()), api_app, )
    # Start API Server
    api_server.serve_forever()


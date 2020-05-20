#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
    Check all the server setups and look for any dysfunctional behavior.
    The following modules will be checked:
    - MongoDB connectivity
    - InfluxDB Connectivity
    - UGE Restful API
    - Provenance Sched Service on UGE Server
    - Provenance Lustre Agent Accessibility

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, exceptions
from getopt import getopt, GetoptError
from influxdb import InfluxDBClient
from pymongo import MongoClient
import urllib.request, json
import socket
import sys
# Access to the 'packages' directory for all python files
sys.path.append('../packages/')
from communication import ServerConnection, CommunicationExp
from config import ServerConfig, ConfigReadExcetion


def main(argv):
    """
    The main method
    :param argv: Command line arguments
    """
    help_msg = """
 Welcome to Provenance Server Setup Verification command. Before running this command make
 sure the server.conf file it setup correctly. In order to check and verify different modules 
 in the Server Stack, you may specifiy all or any of these functions:
 
    -h, --help: Show this help
    -m, --mongodb : Verify connectivity and accessibility to MongoDB
    -i, --influxdb : Verify connectivity and accessibility to InfluxDB
    -u, --ugerest : Verify connectivity and accessibility to UGE Restful API
    -s, --sched : Verify connectivity and accessibility to Provenance Sched Service
    -l, --lustre: Verify connectivity and accessibility to all Lustre Agent services
    -a, --all: Verify everything 
    """
    try:
        # Available functions for this command
        functions = {'help': False, 'mongodb': False, 'influxdb': False, 'ugerest': False,
                   'sched': False, 'lustre': False, 'all': False}
        # Read Commandline arguments
        opts,_ = getopt(argv, 'mishalu', list(functions.keys()))

        # Set the flags for the functions that are requested
        for opt,_ in opts:
            for key in functions.keys():
                if opt in [f"--{key}", f"-{key[0]}"]:
                    functions[key] = True
                    break

        # At least one option has to be selected
        if not any(list(functions.values())):
            print("[Error] At least one option has to be selected\n" + help_msg)

        if functions.pop('help'):
            # Only show the help message
            print(help_msg)

        elif functions.pop('all'):
            # Execute all verification tests
            for key in functions.keys():
                globals()[key]()
        else:
            # Execute test for those modules that are selected
            for key, flag in functions.items():
                if flag: globals()[key]()

    except GetoptError:
        print("[Error] Wrong option. Please see below:\n" + help_msg)
        sys.exit(1)

def mongodb():
    """
        Test MongoDB Connectivity and Accessibility
    """
    print("Test MongoDB Connection... ", end='', flush=True)
    mongoClient = None
    try:
        mongoClient = MongoClient(host=config.getMongoHost(),
                                  port=config.getMongoPort(),
                                  document_class=dict,
                                  username=config.getMongoUser(),
                                  password=config.getMongoPass(),
                                  authSource=config.getMongoSrcDB(),
                                  authMechanism=config.getMongoAuthMode(),
                                  socketTimeoutMS=5000,
                                  connectTimeoutMS=5000,
                                  serverSelectionTimeoutMS=5000)

        mongo_db = mongoClient[config.getMongoSrcDB()]
        mongo_db.collection_names()
        print(f"{OK}")

    except Exception as exp:
        print(f"{FAILED}\n- {exp}")

    finally:
        if mongoClient:
            mongoClient.close()

def influxdb():
    """
        Test InfluxDB Connectivity and Accessibility
    """
    print("Test InfluxDB Connection... ", end='', flush=True)
    influxdbClient = None
    try:
        influxdbClient = InfluxDBClient(
            host=config.getInfluxdbHost(),
            port=config.getInfluxdbPort(),
            username=config.getInfluxdbUser(),
            password=config.getInfluxdbPass(),
            database=config.getInfluxdb_DB()
        )
        influxdbClient.get_list_database()
        print(f"{OK}")

    except Exception as exp:
        print(f"{FAILED}\n- {exp}")

    finally:
        if influxdbClient:
            influxdbClient.close()

def ugerest():
    """
        Test InfluxDB Connectivity and Accessibility
    """
    print("Test UGE RestAPI Availability... ", end='', flush=True)
    ip_list = config.getUGE_Addr()
    port_list = config.getUGE_Port()

    try:
        for inx, ip in enumerate(ip_list):
            ugerest_url = "http://" + ip + ":" + port_list[inx]
            response = urllib.request.urlopen(ugerest_url + "/jobs")

            if response:
                data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))

                if 'errorCode' in data:
                    print(f"{FAILED}\n- [{data['errorCode']}]: {data['errorMessage']}")

            else:
                print(f"{FAILED}\n- No Response from {ugerest_url}")

            print(f"{OK}")

    except Exception as exp:
        print(f"{FAILED}\n- {exp}")


def sched():
    """
        Test RPC communication between the Server and Provenance Sched Service
    """
    try:
        serverCon = ServerConnection(is_rpc=True)
        # Get all the clusters
        clusters = config.getUGE_clusters()
        # Send RPC message to all clusters and wait for response
        for cluster in clusters:
            print(f"Send RPC request to [{cluster}]... ", end='', flush=True)
            # Define the RPC queue
            rpc_queue = '_'.join([cluster, 'rpc', 'queue'])
            # Add the hostname to the request
            request = socket.gethostname()
            # Send/Receive RPC call
            response = serverCon.rpc_call(rpc_queue, request)
            print(f"[\033[92mOK\033[0m]\n - {response}")

    except CommunicationExp as commExp:
        print(f"{FAILED}\n- {commExp}")

    except ConfigReadExcetion as configExp:
        print(f"{FAILED}\n- {configExp}")

    except Exception as exp:
        print(f"{FAILED}\n- {exp}")

def lustre():
    """
        Test Connection between the Server and all Lustre agents
    """
    # Get all the lustre servers from Config file
    lustre_servers = config.getOSS_hosts() + config.getMDS_hosts()
    count_msg = 0

    def callback(ch, method, properties, body):
        try:
            msg = body.decode("utf-8")
            if msg in lustre_servers:
                print(f" - Revieved a message from [{msg}]")
            else:
                print(f" - [Wrong Host]: Received a message from [{msg}]")

            nonlocal count_msg
            count_msg += 1
            if count_msg == len(lustre_servers):
                ch.stop_consuming()

        except Exception as exep:
            ch.stop_consuming()
            print(f"Test Lustre Agent Communication... {FAILED}\n- {exep}")

    try:
        print("Wait for all Lustre Agents to send a message:")
        # Get RabbitMQ params
        server = config.getServer()
        port = config.getPort()
        username = config.getUsername()
        password = config.getPassword()
        Vhost = config.getVhost()
        # Create Credentials with username/password
        credentials = PlainCredentials(username, password)
        # Setup the connection parameters
        params = ConnectionParameters(host=server, port=port, virtual_host=Vhost, credentials=credentials)
        # establish a connection
        conn = BlockingConnection(params)
        # Open a channel
        channel = conn.channel()
        # Declare a direct Exchange for this listener
        channel.exchange_declare(exchange="test", exchange_type='direct')
        # Declare the queue
        channel.queue_declare(queue="test")
        # Bind Queue to Exchange
        channel.queue_bind(exchange="test", queue="test")
        channel.basic_qos(prefetch_count=1)
        # Set basic consumer with a "callback" function
        channel.basic_consume(queue="test", on_message_callback=callback, auto_ack=True)
        channel.start_consuming()
        # Close Connection
        conn.close()

        print("Test Lustre Agent Communication... [\033[92mOK\033[0m]")

    except ConfigReadExcetion as configExp:
        print(f"Test Lustre Agent Communication... {FAILED}\n- {configExp}")

    except (exceptions.ConnectionClosed, exceptions.AMQPConnectionError):
        print(f"Test Lustre Agent Communication... {FAILED}\n- [Connection Error]")

    except exceptions.AMQPChannelError:
        print(f"Test Lustre Agent Communication... {FAILED}\n- [Channel Error]")

    except exceptions.AMQPError:
        print(f"Test Lustre Agent Communication... {FAILED}\n- [AMQP Error]")

    except Exception as exp:
        print(f"Test Lustre Agent Communication... {FAILED}\n- {exp}")

if __name__ == '__main__':
    OK = "[\033[92mOK\033[0m]"
    FAILED = "[\033[91mFAILED\033[0m]"
    try:
        config = ServerConfig()
        main(sys.argv[1:])
    except ConfigReadExcetion as confExp:
        print(f"[Config File Error]: {confExp.getMessage()}")
        sys.exit(1)
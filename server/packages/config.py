# -*- coding: utf-8 -*-
"""
    Read and parse the "../server.conf" file

        1. Read and pars the config file
        2. Validate the config file contents
        3. Apply the modified changes immediately (No need to restart the agent)
        4. Provide external modules with requested parameters

    Misha Ahmadian (misha.ahmadian@ttu.edu)
"""

from configparser import ConfigParser
from typing import Type
import os

# Global static attributes

#
# Main class in Config.py that reads and maintains the configuration
# parameters in memory until the server.config file gets modified
#
class ServerConfig:

    def __init__(self):
        self.__parser = ConfigParser()
        # Track the config file changes
        self.__cached_stamp = 0
        # Config file name and path
        configFile = os.path.dirname(__file__) + '/../conf/server.conf'
        realPath = os.path.realpath(configFile)
        self.__filepath = realPath

    #============= Private Methods =========================================

    # Validate the server.conf to ensure all the mandatory sections and options
    # are defined and correct
    def __validateConfig(self):
        config = {'lustre' : ['mds_hosts', 'oss_hosts', 'jobid_vars', 'mdt_mnt'],
                  'rabbitmq' : ['server', 'port', 'username', 'password', 'vhost', 'prefetch', 'heartbeat', 'timeout'],
                  'io_listener' : ['exchange', 'queue'],
                  'changelogs' : ['files_ops', 'parallel','interval', 'mdt_targets', 'users', 'filter_procs'],
                  'aggregator' : ['interval'],
                  '*uge' : ['clusters', 'address', 'port', 'acct_interval', 'spool_dirs'],
                  'mongodb' : ['host', 'port', 'auth_mode', 'username', 'password', 'database'],
                  'influxdb' : ['host', 'port', 'username', 'password', 'database', 'tzone']}
        # Iterate over the Sections in config file
        for section in config.keys():
            # all the sections with '*' are optional
            if '*' in section:
                # skip the '*'
                section = section[:-1]
                # if the optional section was not provided then continue
                if not self.__parser.has_section(section):
                    continue
            # Else the section is mandatory for configuration
            else:
                if not self.__parser.has_section(section):
                    raise ConfigReadExcetion("The server.conf does not contain [%s] section" % section)

            # Iterate over options under each section of the config file
            for option in config[section]:
                # if optional 'option' is not available then continue
                if not self.__parser.has_option(section, option):
                    continue

                # the mandatory option should be provided
                else:
                    # make sure the mandatory option is provided
                    if not self.__parser.has_option(section, option):
                        raise ConfigReadExcetion("The '%s' is missing under [%s] section in server.conf" \
                                                    % (option, section))

    # Check if config file has been modified since last time
    def isConfigFileModified(self):
        # Check Modification time
        m_stamp = os.stat(self.__filepath).st_mtime
        if m_stamp != self.__cached_stamp:
            # Config File is changed
            self.__cached_stamp = m_stamp
            return True
        # Not Modified
        return False

    # Load and Parse the config file
    # Return True if reload the file in memory
    def __loadConfigFile(self):
        # Check is "server.conf" exist
        if not os.path.isfile(self.__filepath):
            raise ConfigReadExcetion("The config file (server.conf) does not exist")
        # Load the config file if it's been changed so far. Otherwise, keep the
        # file content in memory. the first time it will load the file in memory.
        if self.isConfigFileModified():
            # Read the server.conf file and parse it
            self.__parser.read(self.__filepath)
            # Validate the sections and options
            self.__validateConfig()
            # Return True if reload the file in memory
            return True
        return False

    def __getConfigValue(self, section: str, option: str, retType: Type):
        attrName = section + '_' + option
        # Check if the value has been changed or is not available in this object yet
        # if so, then create it:
        if self.__loadConfigFile() or not hasattr(self, attrName):
            # in case that option is not mandatory and does not exist, then return None
            if not self.__parser.has_option(section, option):
                # we should also check and make sure the section is not optional
                section = '*' + section
                if not self.__parser.has_option(section, option):
                    return None
            # If the value type should be a list
            if retType is list:
                # convert comma separated values of an specific option of specific section to List of values
                tempList = [value.strip() for value in self.__parser.get(section, option).split(',')]
                setattr(self, attrName, tempList)

            # If the value type should be either Integer or Float
            elif retType is int or retType is float:
                try:
                    tempVal = retType(self.__parser.get(section, option))
                    setattr(self, attrName, tempVal)
                except ValueError:
                    raise ConfigReadExcetion("The '{}' parameter under [{}] section must be numeric type of {}"
                                             .format(option, section, retType.__name__))

            # If the value type should be a String
            elif retType is str:
                tempStr = self.__parser.get(section, option)
                setattr(self, attrName, tempStr.strip())

        # Anyway, return the requested value
        return getattr(self, attrName)

    # ============= Public Methods =========================================

    # Get a list of MDS host names defined in Config file
    #   Return: List
    def getMDS_hosts(self) -> list:
        return self.__getConfigValue('lustre', 'mds_hosts', list)

    # Get a list of OSS host names defined in Config file
    #   Return: List
    def getOSS_hosts(self) -> list:
        return self.__getConfigValue('lustre', 'oss_hosts', list)

    # Get a list of jobid_var that are being sent to lustre
    #   Return: List
    def getJobIdVars(self) -> list:
        return self.__getConfigValue('lustre', 'jobid_vars', list)

    # Get the MDT to Mount Point schema
    #   Return: str
    def getMDT_MNT(self) -> str:
        mdt_mnt = self.__getConfigValue('lustre', 'mdt_mnt', str)
        return mdt_mnt.strip("][")

    # Get the name of the server that RabbitMQ-Server is Running
    # Return: String
    def getServer(self) -> str:
        return self.__getConfigValue('rabbitmq', 'server', str)

    # Get the name of the port number of the RabbitMQ-Server
    # Return: String
    def getPort(self) -> int:
        return int(self.__getConfigValue('rabbitmq', 'port', int))

    # Get the username of RabbitMQ-Server
    # Return: String
    def getUsername(self) -> str:
        return self.__getConfigValue('rabbitmq', 'username', str)

    # Get the password of RabbitMQ-Server
    # Return: String
    def getPassword(self) -> str:
        return self.__getConfigValue('rabbitmq', 'password', str)

    # Get the Vhost of the RabbitMQ-Server which handles the Lustre monitoring
    # Return: String
    def getVhost(self) -> str:
        return self.__getConfigValue('rabbitmq', 'vhost', str)

    # Get number of data that has to be prefetched by RabbitMQ
    # Return: integer
    def getPrefetchCount(self) -> int:
        return int(self.__getConfigValue('rabbitmq', 'prefetch', int))

    # Get RabbitMQ connection heartbeat timeout
    # Return: float
    def getHeartBeat(self) -> float:
        return int(self.__getConfigValue('rabbitmq', 'heartbeat', float))

    # Get RabbitMQ blocked connection timeout
    # Return: float
    def getBlockedConnTimeout(self) -> float:
        return int(self.__getConfigValue('rabbitmq', 'timeout', float))

    # Get the name of the Queue that io_listener uses
    # Return: String
    def getIOListener_Queue(self) -> str:
        return self.__getConfigValue('io_listener', 'queue', str)

    # Get the name of the Exchange that io_listener uses
    # Return: String
    def getIOListener_Exch(self) -> str:
        return self.__getConfigValue('io_listener', 'exchange', str)

    # Get the list of File Operations that have to be captured
    # Return: List
    def getChLogsFileOPs(self) -> list:
        return self.__getConfigValue('changelogs', 'files_ops', list)

    # Get number of process that can be running in parallel to collect ChangeLogs
    # Return: Int
    def getChLogsPocnum(self) -> int:
        return self.__getConfigValue('changelogs', 'parallel', int)

    # Get the interval between collecting Lustre ChangeLogs
    # Return: Float
    def getChLogsIntv(self) -> float:
        return self.__getConfigValue('changelogs', 'interval', float)

    # Get a list of Lustre fsname(s) defined in Config file
    #   Return: List
    def getMdtTargets(self) -> list:
        return self.__getConfigValue('changelogs', 'mdt_targets', list)

    # Get the list of ChangeLogs users defined for each MDT
    # Return: List
    def getChLogsUsers(self) -> list:
        return self.__getConfigValue('changelogs', 'users', list)

    # Include the File OPs for non-job processes
    # Return: boolean
    def isFilterProcs(self) -> bool:
        filterProc = self.__getConfigValue('changelogs', 'filter_procs', str)
        if filterProc.lower() == "true":
            return True
        elif filterProc.lower() == "false":
            return False
        else:
            raise ConfigReadExcetion("The 'filter_procs' parameter under [changelogs] section must be True/False")

    # Get the interval between aggregating the received data in the queue
    # Return: Float
    def getAggrIntv(self) -> float:
        return self.__getConfigValue('aggregator', 'interval', float)

    # Get the Aggregator timer Interval
    # Return: Float
    # def getAggrTimer(self) -> float:
    #     return self.__getConfigValue('aggregator', 'timer_intv', float)

    # Get the list of UGE clusters
    #   Return: List
    def getUGE_clusters(self) -> list:
        return self.__getConfigValue('uge', 'clusters', list)

    # Get UGE_REST API address
    # return list
    def getUGE_Addr(self) -> list:
        return self.__getConfigValue('uge', 'addresses', list)

    # Get UGE API port
    # return list
    def getUGE_Port(self) -> list:
        return self.__getConfigValue('uge', 'ports', list)

    # Get List of UGE spool directories
    # return list
    def getUGE_spool_dirs(self) -> list:
        return self.__getConfigValue('uge', 'spool_dirs', list)

    # Get the RPC Calls Interval for collecting UGE accounting data
    # Return: Float
    def getUGEAcctRPCIntv(self) -> float:
        return self.__getConfigValue('uge', 'acct_interval', float)

    # Get the hostname of MongoDB
    # Return: String
    def getMongoHost(self) -> str:
        return self.__getConfigValue('mongodb', 'host', str)

    # Get the hot port number of MongoDB
    # Return: int
    def getMongoPort(self) -> int:
        return self.__getConfigValue('mongodb', 'port', int)

    # Get the MongoDB authentication mode
    # Return: String
    def getMongoAuthMode(self) -> str:
        return self.__getConfigValue('mongodb', 'auth_mode', str)

    # Get the MongoDB username
    # Return: String
    def getMongoUser(self) -> str:
        return self.__getConfigValue('mongodb', 'username', str)

    # Get the MongoDB password
    # Return: String
    def getMongoPass(self) -> str:
        return self.__getConfigValue('mongodb', 'password', str)

    # Get the MongoDB source database
    # Return: String
    def getMongoSrcDB(self) -> str:
        return self.__getConfigValue('mongodb', 'database', str)

    # Get the hostname of InfluxDB
    # Return: String
    def getInfluxdbHost(self) -> str:
        return self.__getConfigValue('influxdb', 'host', str)

    # Get the hot port number of InfluxDB
    # Return: int
    def getInfluxdbPort(self) -> int:
        return self.__getConfigValue('influxdb', 'port', int)

    # Get the InfluxDB username
    # Return: String
    def getInfluxdbUser(self) -> str:
        return self.__getConfigValue('influxdb', 'username', str)

    # Get the InfluxDB password
    # Return: String
    def getInfluxdbPass(self) -> str:
        return self.__getConfigValue('influxdb', 'password', str)

    # Get the InfluxDB source database
    # Return: String
    def getInfluxdb_DB(self) -> str:
        return self.__getConfigValue('influxdb', 'database', str)

    # Get the current time zone
    # Return: String
    def getTimeZone(self) -> str:
        return self.__getConfigValue('influxdb', 'tzone', str)

#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class ConfigReadExcetion(Exception):
    def __init__(self, message):
        self.message = "\n [Error] _CONFIG_: " + message + "\n"
        super(ConfigReadExcetion, self).__init__(self.message)

    def getMessage(self):
        return self.message

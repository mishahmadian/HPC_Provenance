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
import os
import time

#
# Main class in Config.py that reads and maintains the configuration
# parameters in memory until the server.config file gets modified
#
class ServerConfig:

    # Private Constructor
    def __init__(self):
        self.__parser = ConfigParser()
        # Track the config file changes
        self.__cached_stamp = 0
        # Config file name and path
        configFile = os.path.dirname(__file__) + '/../conf/server.conf'
        realPath = os.path.realpath(configFile)
        self.__filepath = realPath

    # Validate the server.conf to ensure all the mandatorysections and options
    # are defined and correct
    def __validateConfig(self):
        config = {'lustre' : ['mds_hosts', 'oss_hosts', 'mdt_targets'],
                  'rabbitmq' : ['server', 'username', 'password'],
                  'io_listener' : ['exchange', 'queue'],
                  'changelogs' : ['interval'],
                  'aggregator' : ['interval', 'timer_intv']}
        # Iterate over the Sections in config file
        for section in config.keys():
            if not self.__parser.has_section(section):
                raise ConfigReadExcetion("The server.conf does not contain [%s] section" % section)
            # Iterate over options under each section of the config file
            for option in config[section]:
                if not self.__parser.has_option(section, option):
                    raise ConfigReadExcetion("The '%s' is missing under [%s] section in server.conf" \
                                                % (option, section))

    # Check if config file has been modified since last time
    def __isCondigFileModified(self):
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
        # Load the confg file if it's been changed so far. Otherwise, keep the
        # file content in memory. the first time it will load the file in memory.
        if self.__isCondigFileModified():
            # Read the server.conf file and parse it
            self.__parser.read(self.__filepath)
            # Validate the sections and options
            self.__validateConfig()
            # Return True if reload the file in memory
            return True
        return False


    # Get a list of Lustre fsname(s) defined in Config file
    #   Return: List
    def getMdtTargets(self):
        mdtTargets = None
        if self.__loadConfigFile() or not hasattr(self, 'mdtTargets'):
            mdtTargets = [mdtTarget.strip() for mdtTarget in self.__parser.get('lustre', 'mdt_targets').split(',')]
        return mdtTargets

    # Get a list of MDS host names defined in Config file
    #   Return: List
    def getMDS_hosts(self):
        MDShosts = None
        if self.__loadConfigFile() or not hasattr(self, 'MDShosts'):
            MDShosts = [fsname.strip() for fsname in self.__parser.get('lustre', 'mds_hosts').split(',')]
        return MDShosts

    # Get a list of OSS host names defined in Config file
    #   Return: List
    def getOSS_hosts(self):
        OSSHosts = None
        if self.__loadConfigFile() or not hasattr(self, 'OSSHosts'):
            OSSHosts = [fsname.strip() for fsname in self.__parser.get('lustre', 'oss_hosts').split(',')]
        return OSSHosts

    # Get the name of the server that RabbitMQ-Server is Running
    # Return: String
    def getServer(self):
        serverName = None
        if self.__loadConfigFile() or not hasattr(self, 'serverName'):
            serverName = self.__parser.get('rabbitmq', 'server')
        return serverName

    # Get the name of the port number of the RabbitMQ-Server
    # Return: String
    def getPort(self):
        serverPort = None
        if self.__loadConfigFile() or not hasattr(self, 'serverPort'):
            serverPort = self.__parser.get('rabbitmq', 'port')
            if not serverPort.isdigit():
                raise ConfigReadExcetion("The 'port' parameter under [rabbitmq] section must be numeric")
        return serverPort

    # Get the username of RabbitMQ-Server
    # Return: String
    def getUsername(self):
        serverUsername = None
        if self.__loadConfigFile() or not hasattr(self, 'serverUsername'):
            serverUsername = self.__parser.get('rabbitmq', 'username')
        return serverUsername

    # Get the password of RabbitMQ-Server
    # Return: String
    def getPassword(self):
        serverPassword = None
        if self.__loadConfigFile() or not hasattr(self, 'serverPassword'):
            serverPassword = self.__parser.get('rabbitmq', 'password')
        return serverPassword

    # Get the Vhost of the RabbitMQ-Server which handles the Lustre monitoring
    # Return: String
    def getVhost(self):
        virtualHost = None
        if self.__loadConfigFile() or not hasattr(self, 'virtualHost'):
            virtualHost = self.__parser.get('rabbitmq', 'vhost')
        return virtualHost

    # Get the name of the Queue that io_listener uses
    # Return: String
    def getIOListener_Queue(self):
        IOLisnQueue = None
        if self.__loadConfigFile() or not hasattr(self, 'IOLisnQueue'):
            IOLisnQueue = self.__parser.get('io_listener', 'queue')
        return IOLisnQueue

    # Get the name of the Exchange that io_listener uses
    # Return: String
    def getIOListener_Exch(self):
        IOLisnExchange = None
        if self.__loadConfigFile() or not hasattr(self, 'IOLisnExchange'):
            IOLisnExchange = self.__parser.get('io_listener', 'exchange')
        return IOLisnExchange

    # Get the interval between collecting Lustre changelogs
    # Return: Float
    def getChLogsIntv(self):
        chlogIntv = None
        if self.__loadConfigFile() or not hasattr(self, 'chlogIntv'):
            chlogIntv = self.__parser.get('changelogs', 'interval')
            if not chlogIntv.isdigit():
                raise ConfigReadExcetion("The 'interval' parameter under [changelogs] section must be numeric")
        return float(chlogIntv)

    # Get the interval between aggregating the received data in the queue
    # Return: Float
    def getAggrIntv(self):
        aggrIntv = None
        if self.__loadConfigFile() or not hasattr(self, 'aggrIntv'):
            aggrIntv = self.__parser.get('aggregator', 'interval')
            if not aggrIntv.isdigit():
                raise ConfigReadExcetion("The 'interval' parameter under [aggregator] section must be numeric")
        return float(aggrIntv)

    # Get the Aggregator timer Interval
    # Return: Float
    def getAggrTimer(self):
        aggrtimer = None
        if self.__loadConfigFile() or not hasattr(self, 'aggrtimer'):
            aggrtimer = self.__parser.get('aggregator', 'timer_intv')
            if not aggrtimer.isdigit():
                raise ConfigReadExcetion("The 'timer_intv' parameter under [aggregator] section must be numeric")
        return float(aggrtimer)

#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class ConfigReadExcetion(Exception):
    def __init__(self, message):
        super(ConfigReadExcetion, self).__init__(message)
        self.message = "\n [Error] _CONFIG_: " + message + "\n"

    def getMessage(self):
        return self.message

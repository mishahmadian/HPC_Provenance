# -*- coding: utf-8 -*-
"""
    Read and parse the "../agent.conf" file

        1. Read and pars the config file
        2. Validate the config file contents
        3. Apply the modified changes immediately (No need to restart the agent)
        4. Provide external modules with requested parameters

    Misha Ahmadian (misha.ahmadian@ttu.edu)
"""

from ConfigParser import SafeConfigParser
import os
import time

#
# Main class in Config.py that reads and maintains the configuration
# parameters in memory until the agent.config file gets modified
#
class AgentConfig:

    # Private Constructor
    def __init__(self):
        self.__parser = SafeConfigParser()
        # Track the config file changes
        self.__cached_stamp = 0
        # Config file name and path
        configFile = os.path.dirname(__file__) + '/../conf/agent.conf'
        realPath = os.path.realpath(configFile)
        self.__filepath = realPath

    # Validate the agent.conf to ensure all the mandatorysections and options
    # are defined and correct
    def __validateConfig(self):
        config = {'lustre' : ['mds_hosts', 'oss_hosts', 'interval'],
                  'rabbitmq' : ['server', 'username', 'password'],
                  'producer' : ['exchange', 'queue', 'delay'],
                  'stats': ['cpu_load', 'mem_usage']}
        # Iterate over the Sections in config file
        for section in config.keys():
            if not self.__parser.has_section(section):
                raise ConfigReadExcetion("The agent.conf does not contain [%s] section" % section)
            # Iterate over options under each section of the config file
            for option in config[section]:
                if not self.__parser.has_option(section, option):
                    raise ConfigReadExcetion("The '%s' is missing under [%s] section in agent.conf" \
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
        # Check is "agent.conf" exist
        if not os.path.isfile(self.__filepath):
            raise ConfigReadExcetion("The config file (agent.conf) does not exist")
        # Load the confg file if it's been changed so far. Otherwise, keep the
        # file content in memory. the first time it will load the file in memory.
        if self.__isCondigFileModified():
            # Read the agent.conf file and parse it
            self.__parser.read(self.__filepath)
            # Validate the sections and options
            self.__validateConfig()
            # Return True if reload the file in memory
            return True
        return False


    # Get a list of MDS host names defined in Config file
    #   Return: List
    def getMDS_hosts(self):
        if self.__loadConfigFile() or not hasattr(self, 'MDShosts'):
            self.MDShosts = [fsname.strip() for fsname in self.__parser.get('lustre', 'mds_hosts').split(',')]
        return self.MDShosts

    # Get a list of OSS host names defined in Config file
    #   Return: List
    def getOSS_hosts(self):
        if self.__loadConfigFile() or not hasattr(self, 'OSSHosts'):
            self.OSSHosts = [fsname.strip() for fsname in self.__parser.get('lustre', 'oss_hosts').split(',')]
        return self.OSSHosts

    # Get the interval between jobstat collecting process
    # Return: Float
    def getJobstatsInterval(self):
        if self.__loadConfigFile() or not hasattr(self, 'jstatsInt'):
            self.jstatsInt = self.__parser.get('lustre', 'interval')
            if not self.jstatsInt.isdigit():
                raise ConfigReadExcetion("The 'interval' parameter under [lustre] section must be numeric")
        return float(self.jstatsInt)

    # Get the interval between jobstat collecting process
    # Return: String
    def getMaxJobstatAge(self):
        if self.__loadConfigFile() or not hasattr(self, 'jstatsAge'):
            self.jstatsAge = self.__parser.get('lustre', 'max_age')
            if not self.jstatsAge.isdigit():
                raise ConfigReadExcetion("The 'max_age' parameter under [lustre] section must be numeric")
        return self.jstatsAge

    # Get the name of the server that RabbitMQ-Server is Running
    # Return: String
    def getServer(self):
        if self.__loadConfigFile() or not hasattr(self, 'serverName'):
            self.serverName = self.__parser.get('rabbitmq', 'server')
        return self.serverName

    # Get the name of the port number of the RabbitMQ-Server
    # Return: String
    def getPort(self):
        if self.__loadConfigFile() or not hasattr(self, 'serverPort'):
            self.serverPort = self.__parser.get('rabbitmq', 'port')
        return self.serverPort

    # Get the username of RabbitMQ-Server
    # Return: String
    def getUsername(self):
        if self.__loadConfigFile() or not hasattr(self, 'serverUsername'):
            self.serverUsername = self.__parser.get('rabbitmq', 'username')
        return self.serverUsername

    # Get the password of RabbitMQ-Server
    # Return: String
    def getPassword(self):
        if self.__loadConfigFile() or not hasattr(self, 'serverPassword'):
            self.serverPassword = self.__parser.get('rabbitmq', 'password')
        return self.serverPassword

    # Get the Vhost of the RabbitMQ-Server which handles the Lustre monitoring
    # Return: String
    def getVhost(self):
        if self.__loadConfigFile() or not hasattr(self, 'virtualHost'):
            self.virtualHost = self.__parser.get('rabbitmq', 'vhost')
        return self.virtualHost

    # Get the name of the Queue that producer uses
    # Return: String
    def getProd_Queue(self):
        if self.__loadConfigFile() or not hasattr(self, 'prodQueue'):
            self.prodQueue = self.__parser.get('producer', 'queue')
        return self.prodQueue

    # Get the name of the Exchange that producer uses
    # Return: String
    def getProd_Exch(self):
        if self.__loadConfigFile() or not hasattr(self, 'prodExchange'):
            self.prodExchange = self.__parser.get('producer', 'exchange')
        return self.prodExchange

    # Get the delay interval between sending the logs to server
    # Return: Float
    def getSendingInterval(self):
        if self.__loadConfigFile() or not hasattr(self, 'sendIntv'):
            self.sendIntv = self.__parser.get('producer', 'delay')
            if not self.sendIntv.isdigit():
                raise ConfigReadExcetion("The 'delay' parameter under [producer] section must be numeric")
        return float(self.sendIntv)

    # The agent should/not collect CPU Load of the host
    # Return: Boolean
    def is_CPU_Load_avail(self):
        if self.__loadConfigFile() or not hasattr(self, 'cpuLoad'):
            cpuLoad = self.__parser.get('stats', 'cpu_load')
            if cpuLoad.lower() == 'true':
                return True
            elif cpuLoad.lower() == 'false':
                return False
            else:
                raise ConfigReadExcetion("The 'cpu_load' parameter under [stats] section must be True/False")

    # The agent should/not collect CPU Load of the host
    # Return: Boolean
    def is_Mem_Usage_avail(self):
        if self.__loadConfigFile() or not hasattr(self, 'memUsage'):
            memUsage = self.__parser.get('stats', 'mem_usage')
            if memUsage.lower() == 'true':
                return True
            elif memUsage.lower() == 'false':
                return False
            else:
                raise ConfigReadExcetion("The 'mem_usage' parameter under [stats] section must be True/False")

#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class ConfigReadExcetion(Exception):
    def __init__(self, message):
        self.message = "\n [Error] _CONFIG_: " + message + "\n"
        super(ConfigReadExcetion, self).__init__(self.message)

    def getMessage(self):
        return self.message

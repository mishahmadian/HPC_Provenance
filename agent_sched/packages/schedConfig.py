# -*- coding: utf-8 -*-
"""
    Read and parse the "../conf/sched.conf" file

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
class SchedConfig:

    def __init__(self):
        self.__parser = ConfigParser()
        # Track the config file changes
        self.__cached_stamp = 0
        # Config file name and path
        configFile = os.path.dirname(__file__) + '/../conf/sched.conf'
        realPath = os.path.realpath(configFile)
        self.__filepath = realPath

    #============= Private Methods =========================================

    # Validate the server.conf to ensure all the mandatory sections and options
    # are defined and correct
    def __validateConfig(self):
        config = {'rabbitmq' : ['server', 'port', 'username', 'password', 'vhost', 'rpc_queue'],
                  '*uge_acct' : ['accounting_file', 'max_read_line']}
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

    # Get the Vhost of the RabbitMQ-Server
    # Return: String
    def getVhost(self) -> str:
        return self.__getConfigValue('rabbitmq', 'vhost', str)

    # Get the RPC_Queue name of the RabbitMQ-Server
    # Return: String
    def getRPC_queue(self) -> str:
        return self.__getConfigValue('rabbitmq', 'rpc_queue', str)

    # Get path to UGE accounting file
    # Return: String
    def getUGE_acct_file(self) -> str:
        return self.__getConfigValue('uge_acct', 'accounting_file', str)

    # Get the name of the port number of the RabbitMQ-Server
    # Return: String
    def getMaxReadLine(self) -> int:
        return int(self.__getConfigValue('uge_acct', 'max_read_line', int))


#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class ConfigReadExcetion(Exception):
    def __init__(self, message):
        self.message = "\n [Error] _SDHED_CONFIG_: " + message + "\n"
        super(ConfigReadExcetion, self).__init__(self.message)


    def getMessage(self):
        return self.message

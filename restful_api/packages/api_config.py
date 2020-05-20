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
import codecs
import os

# Global static attributes

#
# Main class in Config.py that reads and maintains the configuration
# parameters in memory until the server.config file gets modified
#
class Config:

    def __init__(self):
        self.__parser = ConfigParser()
        # Track the config file changes
        self.__cached_stamp = 0
        # Config file name and path
        configFile = os.path.dirname(__file__) + '/../conf/rest_api.conf'
        realPath = os.path.realpath(configFile)
        self.__filepath = realPath

    #============= Private Methods =========================================

    # Validate the server.conf to ensure all the mandatory sections and options
    # are defined and correct
    def __validateConfig(self):
        config = {'api': ['port'],
                  'mongodb' : ['host', 'port', 'auth_mode', 'username', 'password', 'database'],
                  'lustre_schema' : ['schema']}
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
            raise ConfigReadExcetion("The config file (api_config.conf) does not exist")
        # Load the config file if it's been changed so far. Otherwise, keep the
        # file content in memory. the first time it will load the file in memory.
        if self.isConfigFileModified():
            # Read the server.conf file and parse it
            #self.__parser.read(self.__filepath)
            self.__parser.read_file(open(self.__filepath))
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
                tempStr.replace('\\n', '\n')
                setattr(self, attrName, tempStr.strip())

        # Anyway, return the requested value
        return getattr(self, attrName)

    # ============= Public Methods =========================================
    # Get the api port number
    # Return: int
    def getApiPort(self) -> int:
        return self.__getConfigValue('api', 'port', int)
    # Get the hostname of MongoDB
    # Return: String
    def getMongoHost(self) -> str:
        return self.__getConfigValue('mongodb', 'host', str)

    # Get the port number of MongoDB
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

    def getSchema(self) -> str:
        schema = self.__getConfigValue('lustre_schema', 'schema', str)
        return schema.strip("][")



#
# In any case of Error, Exception, or Mistake ConfigReadExcetion will be raised
#
class ConfigReadExcetion(Exception):
    def __init__(self, message):
        self.message = "\n [Error] _CONFIG_: " + message + "\n"
        super(ConfigReadExcetion, self).__init__(self.message)

    def getMessage(self):
        return self.message

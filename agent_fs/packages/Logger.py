# -*- coding: utf-8 -*-
"""
    This module will log the errors and will save them under the "log" directory
    ** This module had to be modified to be functional with Python2

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from datetime import datetime
import os.path as Path
import socket
import fcntl
import os

# Global variables
_base_log_name = 'Provenance_lustre_'
_base_log_suffix = '.log'
_max_log_file = 3   # Keep the logs for 6 months
_enabled = 1

# The main function to eb used for logging
#
def log(log_mode, log_message):
    """
    This function will be called by different classes to save their error states
    :return: None
    """
    # This line if for test cases
    if not _enabled:
        print("[{}] {}".format(log_mode, log_message))
        return
    # Append the Logging mode to the message
    if log_mode not in [Mode.APP_START, Mode.APP_EXIT]:
        log_message = "({}) {}".format(log_mode, log_message)
    else:
        hostname = socket.gethostname()
        log_message = "(host: {}) {}".format(hostname, log_message)
    # Get the current date/time of the log
    log_date = datetime.now().strftime("%m-%d-%Y, %H:%M:%S")
    # append the dat/time to message
    log_message = "[{}] {}".format(log_date, log_message)
    #
    # Open the file in this way helps to apply flock to the file
    logFile = open(_get_log_file(), 'a')
    # lock the file to prevent race condition between processes
    fcntl.flock(logFile, fcntl.LOCK_EX)
    try:
        logFile.write(log_message + "\n")
    finally:
        fcntl.flock(logFile, fcntl.LOCK_UN)
        logFile.close()

#
# Find/Create the log file of this month
#
def _get_log_file():
    """
    This local function will find the correct log file for this month, if it exists
    returns the file, otherwise first will look insie the log directory, if the number
    of current log files is >= _max_log_file the removes the oldest lof file and then
    creates a log file for this month

    :return: the String path of the current log file (We don't use the Pathlib format
             since we are going to apply flock on files to avoid race condition
    """
    # get the location of the log file directory: ../config
    log_dir = Path.join(Path.dirname(Path.dirname(Path.abspath(__file__))), 'logs')
    # Make the log directory if does not exist
    if not Path.exists(log_dir):
        os.mkdir(log_dir)
    # The current Month and Year for this log
    current_M_Y = datetime.today().strftime("%m-%Y")
    # The log file that has to be used for this month
    current_log = Path.join(log_dir, _base_log_name + current_M_Y + _base_log_suffix)
    # if the current_log does not exist, then rotate the log (if necessary)
    # and create the log file for this month
    if not Path.exists(current_log):
        # Get the list of log directory contents
        log_dir_ls = os.listdir(log_dir)
        # Count the number of log files in this directory
        # we aim to keep not more than 10 log files in this directory
        if len(log_dir_ls) >= _max_log_file:
            log_dir_files = []
            for logfile in log_dir_ls:
                logfilePath = Path.join(log_dir, logfile)
                log_dir_files.append((os.stat(logfilePath).st_mtime, logfilePath))

            # find the oldest log file
            _, old_log = min(logTuple for logTuple in log_dir_files)
            # Delete the old log file
            os.remove(old_log)

    # Return the log file
    return current_log

#
# The Supported modes for this logger
#
class Mode:
    APP_START = 'APP_START'
    APP_EXIT = 'APP_EXIT'
    COMMUNICATION = 'COMMUNICATION'
    CONFIG = 'CONFIG'
    IO_COLLECTOR = 'IO_COLLECTOR'


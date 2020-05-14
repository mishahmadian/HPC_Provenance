# -*- coding: utf-8 -*-
"""
    This module will log the errors and will save them under the "log" directory

 Misha Ahmadian (misha.ahmadian@ttu.edu)
"""
from collections import Counter
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
import fcntl

# Global variables
_base_log_name = 'Provenance_sched_'
_base_log_suffix = '.log'
_max_log_file = 6   # Keep the logs for 6 months

#
# The main function to eb used for logging
#
def log(log_mode: 'Mode', log_message :str) -> None:
    """
    This function will be called by different classes to save their error states
    :return: None
    """
    # Append the Logging mode to the message
    if log_mode not in [Mode.APP_START, Mode.APP_EXIT]:
        log_message = f"({log_mode.name}) {log_message}"
    # Get the current date/time of the log
    log_date = datetime.now().strftime("%m-%d-%Y, %H:%M:%S")
    # append the dat/time to message
    log_message = f"[{log_date}] {log_message}"
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
def _get_log_file() -> str:
    """
    This local function will find the correct log file for this month, if it exists
    returns the file, otherwise first will look insie the log directory, if the number
    of current log files is >= _max_log_file the removes the oldest lof file and then
    creates a log file for this month

    :return: the String path of the current log file (We don't use the Pathlib format
             since we are going to apply flock on files to avoid race condition
    """
    # get the location of the log file directory: ../config
    log_dir = Path(__file__).parent.parent.joinpath("logs")
    # Make the log directory if does not exist
    log_dir.mkdir(exist_ok=True)
    # The current Month and Year for this log
    current_M_Y = datetime.today().strftime("%m-%Y")
    # The log file that has to be used for this month
    current_log = log_dir.joinpath(_base_log_name + current_M_Y + _base_log_suffix)
    # if the current_log does not exist, then rotate the log (if necessary)
    # and create the log file for this month
    if not current_log.exists():
        # Count the number of log files in this directory
        # we aim to keep not more than 10 log files in this directory
        count_logs = Counter(logfile.suffix for logfile in log_dir.iterdir())
        if count_logs[_base_log_suffix] >= _max_log_file:
            # find the oldest log file
            _, old_log = min((logfile.stat().st_mtime, logfile) for logfile in log_dir.iterdir())
            # Delete the old log file
            old_log.unlink()

        # Create the log file
        current_log.touch()
    # Return the log file
    return current_log.as_posix()

#
# The Supported modes for this logger
#
class Mode(Enum):
    """
        Defines what classes are being logged by this logger
    """
    APP_START = auto()
    APP_EXIT = auto()
    AGGREGATOR = auto()
    COMMUNICATION = auto()
    CONFIG = auto()
    DB_MANAGER = auto()
    DB_OPERATION = auto()
    FILE_IO_STATS = auto
    FILE_OP_LOGS = auto()
    MAIN = auto()
    PERSISTENT = auto()
    SCHEDULER = auto()
    UGE_SERVICE = auto()

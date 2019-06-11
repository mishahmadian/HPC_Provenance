#!/bin/bash
#----------------------------------------#
#         Luster Monitoring Server       #
#               Version 1.0              #
#                                        #
#       Running on Monitoring Server     #
#                                        #
#              Misha Ahmadian            #
#----------------------------------------#
#
# Path ro Python3
PYTHON3_PATH=/usr/bin/python36
# Path ro Server executable
LM_SERVER_EXE=$PWD/bin/lustre_monitoring_server.py
#
if [ -x $PYTHON3_PATH ]; then
	# The major number in Python version
	PY_VER=`$PYTHON3_PATH --version | awk '{split($2,ver,"."); print ver[1]}'`
	# Python version have to be 3+
	if [ $PY_VER -lt 3 ]; then
		echo -e " Error: Python version 3 is required\n"
	fi

	if [ -f $LM_SERVER_EXE ]; then

		$PYTHON3_PATH $LM_SERVER_EXE

	else
		echo -e " Error: The '$LM_SERVER_EXE' does not exist\n"
		exit 2
	fi
else
	echo " Error: Path to Python3 executable is wrong\n"
fi

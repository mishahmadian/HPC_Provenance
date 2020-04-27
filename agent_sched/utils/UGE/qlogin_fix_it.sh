#!/bin/bash
#|==================================================|
#|         High Performance Computing Center        |
#|               Texas Tech University              |
#|                                                  |
#| This script is called by qlogin_wrapper to find  |
#| the corresponding env variables for this job and |
#| assign those variables + JOB_STATS_ID to thos job|
#| at the end it fixed the memory allocation issue  |
#| in qlogin by adding this process to cgroups      |
#|                                                  |
#|                                                  |
#| misha.ahmadian@ttu.edu                           |
#|==================================================|
#
CLUSTER="quanah"
SCHED="uge"
# Fin the right path of the spool directory for this host, since
# UGE has a strange Spool structure on Quanah
QLOGIN_HOST=${HOSTNAME}
if [ -d "/export/uge/default/spool/${QLOGIN_HOST}/${QLOGIN_HOST}" ]; then
	QLOGIN_HOST="${QLOGIN_HOST}/${QLOGIN_HOST}"
fi
#
# Find JOB_ID and Generate $JOB_STATS_ID varialbles
UGE_SPOOL=/export/uge/default/spool/${QLOGIN_HOST}/active_jobs
PPPID="$(ps -p $PPID -o ppid= | xargs)"
JOB_ID=""
NSLOTS=""
#
for dir in `ls $UGE_SPOOL/`; do
        JOB_PID="$(cat $UGE_SPOOL/${dir}/job_pid)"
	# If we could find the matched job_pid with the pid of the shepherd then 
	# it means we can find the variables for this job:
        if [ "$JOB_PID" == "$PPPID" ]; then
                JOB_ID="$(cat $UGE_SPOOL/${dir}/environment | grep JOB_ID | awk -F'=' '{print $2}')"
                NSLOTS="$(cat $UGE_SPOOL/${dir}/environment | grep NSLOTS | awk -F'=' '{print $2}')"
                break
        fi
done
#
export JOB_ID
export NSLOTS
export JOB_STATS_ID=${CLUSTER}_${SCHED}_$JOB_ID
#
# Fix the Memory Quota issue for this Qlogin session
#
JOB_MEM_CGRP_TASK=/sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
if [ -e $JOB_MEM_CGRP_TASK ]; then
	echo "$$" >> /sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
	# This will call the waiting-prolog process to set back the 
	# cgroups permissions to root
	kill -s SIGUSR1 "$(cat /tmp/qlogin_prolog.${JOB_ID})"
fi

#!/bin/bash
#
# Find JOB_ID and Generate $MY_JOB_ID varialbles
UGE_SPOOL=/export/uge/default/spool/$HOSTNAME/active_jobs
PPPID="$(ps -p $PPID -o ppid= | xargs)"
JOB_ID=""
#
for dir in `ls $UGE_SPOOL/`; do
        JOB_PID="$(cat $UGE_SPOOL/${dir}/job_pid)"
        if [ "$JOB_PID" == "$PPPID" ]; then
                JOB_ID="$(cat $UGE_SPOOL/${dir}/environment | grep JOB_ID | awk -F'=' '{print $2}')"
                break
        fi
done
#
export JOB_ID
export MY_JOB_ID=genius_uge_$JOB_ID
#
# Fix the Memory Quota issue for this Qlogin session
#
JOB_MEM_CGRP_TASK=/sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
if [ -e $JOB_MEM_CGRP_TASK ]; then
	echo "$$" >> /sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
fi

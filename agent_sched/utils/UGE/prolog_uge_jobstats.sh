#!/bin/bash
#|==================================================|
#|         High Performance Computing Center        |
#|               Texas Tech University              |
#|                                                  |
#| This Prolog script does the following tasks:     |
#|   1) Add "JOB_STATS_ID" variable to every job    |
#|      to allow JobStat collects job's I/O         |
#|   2) Fix the memory allocation issue in Qlogin   |
#|                                                  |
#|                                                  |
#| misha.ahmadian@ttu.edu                           |
#|==================================================|
#
CLUSTER="quanah"
SCHED="uge"
CURRENTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 || exit ; pwd -P )"
#
# UGE allocates a strange amount of memory when h_vmem is requested by users:
# ((#cores + 1 ) * h_vmem). The following script should fix this for every job
"${CURRENTPATH}"/prolog_fix_hvmem.sh $JOB_ID $SGE_TASK_ID $NSLOTS
#
# The following section will add the JOB_STATS_ID to every job
#
if [ -n "$JOB_SCRIPT" ]; then
	# Exit if job is a QLOGIN call, then ignore it 
	if [ "$JOB_SCRIPT" != "QLOGIN" ] && [ "$JOB_SCRIPT" != "QRSH" ]; then
		# The directory structure of UGE Spool is very strange on Quanah, below we make
		# sure that TARGET_HOST refers to the right path of the current host directory in spool
		TARGET_HOST="${HOSTNAME//.localdomain/}"
		if [ -d "/export/uge/default/spool/${TARGET_HOST}/${TARGET_HOST}" ]; then
          		TARGET_HOST="${TARGET_HOST}/${TARGET_HOST}"
        	fi
		# Define the actual path of the job_script inside the spool directory to compare
		# against the provided JOB_SCRIPT.
		UGE_SPOOL_SCRIPT="/export/uge/default/spool/${TARGET_HOST}/job_scripts/$JOB_ID"
		# Generate the JOB_STATS_ID variable for the Provenance/Lustre_Jobstat
		if [ "$SGE_TASK_ID" != 'undefined' ]; then
			JOB_STATS_ID="${CLUSTER}_${SCHED}_\${JOB_ID}.\${SGE_TASK_ID}"
		else
			JOB_STATS_ID="${CLUSTER}_${SCHED}_\${JOB_ID}"
		fi
		# Add JOB_STATS_ID to JOB_SCRIPT if the it is not a binary job
		# This method comes true if job is not a binary job 
		# (no other way to check binary jobs in Prolog"
		if [ -e "$JOB_SCRIPT" ] && [ "$JOB_SCRIPT" = "$UGE_SPOOL_SCRIPT" ]; then
			# add JOB_STATS_ID to the job script file availabe under UGE spool
			# UGE qalter [-v or -ac] does not work for this case!! 
			# But approach works just fine without modifying user's main script file
			sed -i "1s/^/export JOB_STATS_ID=$JOB_STATS_ID\n/" "$JOB_SCRIPT"
		else
			# Otherwise, for binary jobs create a new bash environment and
			# pass the necessary variables, then run the command
			#echo "BINARY"
			export JOB_STATS_ID=$JOB_STATS_ID
			##$SHELL -c "$JOB_SCRIPT"
			#/export/uge/bin/lx-amd64/qdel $JOB_STATS_ID &>/dev/null
			
		fi
		# Remove JOB_STATS_ID variable
		unset JOB_STATS_ID
	
	elif [ "$JOB_SCRIPT" == "QLOGIN" ]; then
		# ----- If QLOGIN: ------
		# Allow 'qlogin_fix_it' to add the ssh process id into list of UGE memory limits
		# in order to fix the memory quota issue in case that h_vmem is set
		JOB_MEM_CGRP_TASK=/sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
		if [ -e "$JOB_MEM_CGRP_TASK" ]; then
			# Allow the qlogin_wrapper to add its process id into the cgroups
			chown "$SGE_O_LOGNAME" /sys/fs/cgroup/memory/UGE/"${JOB_ID}".1/tasks
			# Run the waiting-for-signal script file in bg and exit
      "${CURRENTPATH}"/prolog_wait_qlogin.sh "$JOB_ID" "$SGE_O_LOGNAME" &
		fi
	fi

fi

#!/bin/bash
#
if [ -n "$JOB_SCRIPT" ]; then
	# Exit if job is a QLOGIN call, then ignore it
	if [ "$JOB_SCRIPT" != "QLOGIN" ]; then
		UGE_SPOOL_SCRIPT="/export/uge/default/spool/${HOSTNAME//.localdomain/}/job_scripts/$JOB_ID"
		# Generate the MY_JOB_ID variable for the Provenance/Lustre_Jobstat
		if [ "$SGE_TASK_ID" != 'undefined' ]; then
			MY_JOB_ID="genius_uge_${JOB_ID}.${SGE_TASK_ID}"
		else
			MY_JOB_ID="genius_uge_${JOB_ID}"
		fi
		# Add MY_JOB_ID to JOB_SCRIPT if it is not a binary job
		# This method comes true if job is not binary 
		# (no other way to check binary jobs in Prolog"
		if [ -e "$JOB_SCRIPT" ] && [ "$JOB_SCRIPT" = "$UGE_SPOOL_SCRIPT" ]; then
			# add MY_JOB_ID to the job script file availabe under UGE spool
			# UGE qalter [-v or -ac] does not work for this case!! 
			# But approach works just fine without modifying user's main script file
			sed -i "1s/^/export MY_JOB_ID=$MY_JOB_ID\n/" "$JOB_SCRIPT"
		else
			# Otherwise, for binary jobs create a new bash environment and
			# pass the necessary variables, then run the command
			echo "BINARY"
			export MY_JOB_ID=$MY_JOB_ID
			$SHELL -c "$JOB_SCRIPT"
			#/export/uge/bin/lx-amd64/qdel $MY_JOB_ID &>/dev/null
			
		fi
		# Remove MY_JOB_ID variable
		unset MY_JOB_ID
	
	else
		# ----- If QLOGIN: ------
		# Allow 'qlogin_fix_it' to add the ssh process id into list of UGE memory limits
		# in order to fix the memory quota issue in case that h_vmem is set
		JOB_MEM_CGRP_TASK=/sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
		if [ -e $JOB_MEM_CGRP_TASK ]; then
			chmod o+w /sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks
		fi
	fi

fi

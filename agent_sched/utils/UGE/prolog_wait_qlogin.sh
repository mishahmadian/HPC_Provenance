#!/bin/bash
#|==================================================|
#|         High Performance Computing Center        |
#|               Texas Tech University              |
#|                                                  |
#| This script calls by prolog_uge_jobstats.sh in   |
#| Background to wait for signal from qlogin_wrapper|
#| and fix the cgroups permission to avoid any      |
#| security hole in qlogin.                         |
#|                                                  |
#|                                                  |
#| misha.ahmadian@ttu.edu                           |
#|==================================================|
#
JOB_ID=$1
UGE_USER=$2
#
# This will be running in the background and wait until the
# qlogin_fix_it.sh send a signal to this as a non-root user
# and let this process to run priviledge set_cgroups_perms
# function to avoid any security holes
function wait_for_signal(){
	echo "$$" > /tmp/qlogin_prolog.${JOB_ID} 
	trap "exit 0" SIGUSR1
	# Wait for 60 secs until recieve a SIGUSR1 signal
	sleep 60 &
	wait $!
}
# This function will set back the permisions to cgroups/memory/tasks
# which was changed by Prolog for a UGE job with $JOB_ID
function set_cgroups_perms(){
	cgroup_mem_task="/sys/fs/cgroup/memory/UGE/${JOB_ID}.1/tasks"
	prolog_wait_proc="/tmp/qlogin_prolog.${JOB_ID}"
	# Set back the permission to root
	if [ -e "$cgroup_mem_task" ]; then
        	chown root $cgroup_mem_task
	fi
	# remove the temorary file that keeps the prolog waiting proc
	if [ -e "$prolog_wait_proc" ]; then
		rm -f /tmp/qlogin_prolog.${JOB_ID}
	fi
        exit 0
}
# Let wait_for_signal signal be availabe in another bash 
export -f wait_for_signal
# Allo the user to send a signal to this process
su $UGE_USER -c "bash -c wait_for_signal"
# Set back the root permissions to cgroups/memory/task
# after 60 secs or when a SUGUSR1 signal recieved was
# Recieved from qlogin_fix_it.sh
set_cgroups_perms

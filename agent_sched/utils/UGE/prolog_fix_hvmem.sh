#!/bin/bash
##|==================================================|
##|         High Performance Computing Center        |
##|               Texas Tech University              |
##|                                                  |
##| This script fixes the cgroups memeory limit issue|
##| which is being caused by UGE h_vmem parameter.   |
##|                                                  |
##| misha.ahmadian@ttu.edu                           |
##|==================================================|
#------ Arguments passed to this script--------------
jobid="$1"
taskid="$2"
ncores="$3"
#------ Global Variables ----------------------------
max_num_cores="36"

if [ "$taskid" == "undefined" ]; then
	taskid="1"
fi
cgrp_mem_path="/sys/fs/cgroup/memory/UGE/${jobid}.${taskid}/memory.limit_in_bytes"
cgrp_memsw_path="/sys/fs/cgroup/memory/UGE/${jobid}.${taskid}/memory.memsw.limit_in_bytes"
#
# First we check if the memory limit has been defined for this job
# Due to having JSV in action, this would be impossible to have no
# memory limit, except the jsv has been disabled or overwritten.
if [ -e "$cgrp_mem_path" ] && [ -n "$ncores" ]; then
    # For MPI jobs that number of requested slots exceed $max_num_cores
    # we should consider the $max_num_cores per machine
    if [ "$ncores" -gt "$max_num_cores" ]; then
        ncores=$max_num_cores
    fi
    # Read the current cgroups memory limit for this job which is defined
    # by h_vmem either in jobscript or by JSV.
    cgrp_mem=`cat $cgrp_mem_path`
    # Adjust the memory based on the number of requested cores
    adjusted_mem=`awk -v mem=$cgrp_mem -v cores=$ncores 'BEGIN {printf "%.0f", ((mem * cores) / (cores + 1))}'`
    # Assign a soft limit to memory swap space (add 1G per core to the memory)
    adjusted_memsw=`awk -v mem=$adjusted_mem -v cores=$ncores 'BEGIN {printf "%.0f", (mem + (cores * (1024 ** 3)))}'`
    # Reset the cgroups memory limit with adjusted value
    echo "$adjusted_mem" > $cgrp_mem_path
    # Reset the cgroups memory swap to its soft limit
    echo "$adjusted_memsw" > $cgrp_memsw_path
fi

#!/bin/bash

# Clean MDS:
echo -n "Cleaning MGS Logs..."
ssh root@mgs "lctl set_param mdt.*.job_stats=clear && lctl get_param mdt.*.job_stats" &> /dev/null
echo " Done!"

# Clean OSSs:
for oss in oss1 oss2; do
	echo -n "Cleaning $oss Logs..."
	ssh root@${oss} "lctl set_param obdfilter.*.job_stats=clear && lctl get_param obdfilter.*.job_stats" &> /dev/null
	echo " Done!"
done

# Clean Changelogs
echo -n "Cleaning ChangeLogs..."
lfs changelog_clear test-MDT0000 cl3 0 &> /dev/null
echo " Done!"

# Clean up files
echo -n "Remove output files..."
rm -f ./*.o*
echo " Done!"

# cleanup RabbitMQ queue
rabbitmqctl -p /monitoring purge_queue jobstat
rabbitmqctl -p /monitoring purge_queue genius_rpc_queue

# Clean finished Job DB
echo -n "Remove finished jobs DB..."
rm -f ../packages/fjobids.dat
echo " Done!"

#!/bin/bash

source /usr/workspace/bin/common.sh

vw-cache-clear-all; sleep 45

echo ">>> vw-trace-run -f /usr/workspace//traces/small.trace -l LFU-1 -c /usr/workspace//etc/LFULocal.conf"
vw-trace-run -f /usr/workspace//traces/small.trace -l LFU-1 -c /usr/workspace//etc/LFULocal.conf
echo ">>> Waiting for submission to end..."
WAIT=0
DONE_WAIT=false
while ! $DONE_WAIT;
do
	sleep 30
	WAIT=$(($WAIT + 30))
	QSTAT=$(qstat | wc -c)
	if [ $QSTAT -eq 0 ];
	then
		DONE_WAIT=true
	elif [ $WAIT -gt 216000 ];
	then
		echo 'Waited 1h for this trace to run. Something must be wrong. Aborting!'
		qdel -u root
		DONE_WAIT=true
	fi
done
vw-cache-clear-all; sleep 45
echo ">>> vw-trace-run -f /usr/workspace//traces/small.trace -l LRU-1 -c /usr/workspace//etc/LRULocal.conf"
vw-trace-run -f /usr/workspace//traces/small.trace -l LRU-1 -c /usr/workspace//etc/LRULocal.conf
echo ">>> Waiting for submission to end..."
WAIT=0
DONE_WAIT=false
while ! $DONE_WAIT;
do
	sleep 30
	WAIT=$(($WAIT + 30))
	QSTAT=$(qstat | wc -c)
	if [ $QSTAT -eq 0 ];
	then
		DONE_WAIT=true
	elif [ $WAIT -gt 216000 ];
	then
		echo 'Waited 1h for this trace to run. Something must be wrong. Aborting!'
		qdel -u root
		DONE_WAIT=true
	fi
done
vw-cache-clear-all; sleep 45
echo ">>> vw-trace-run -f /usr/workspace//traces/small.trace -l LFUGlobal-1 -c /usr/workspace//etc/LFUGlobal.conf"
vw-trace-run -f /usr/workspace//traces/small.trace -l LFUGlobal-1 -c /usr/workspace//etc/LFUGlobal.conf
echo ">>> Waiting for submission to end..."
WAIT=0
DONE_WAIT=false
while ! $DONE_WAIT;
do
	sleep 30
	WAIT=$(($WAIT + 30))
	QSTAT=$(qstat | wc -c)
	if [ $QSTAT -eq 0 ];
	then
		DONE_WAIT=true
	elif [ $WAIT -gt 216000 ];
	then
		echo 'Waited 1h for this trace to run. Something must be wrong. Aborting!'
		qdel -u root
		DONE_WAIT=true
	fi
done
vw-cache-clear-all; sleep 45
echo ">>> vw-trace-run -f /usr/workspace//traces/small.trace -l LRUGlobal-1 -c /usr/workspace//etc/LRUGlobal.conf"
vw-trace-run -f /usr/workspace//traces/small.trace -l LRUGlobal-1 -c /usr/workspace//etc/LRUGlobal.conf
echo ">>> Waiting for submission to end..."
WAIT=0
DONE_WAIT=false
while ! $DONE_WAIT;
do
	sleep 30
	WAIT=$(($WAIT + 30))
	QSTAT=$(qstat | wc -c)
	if [ $QSTAT -eq 0 ];
	then
		DONE_WAIT=true
	elif [ $WAIT -gt 216000 ];
	then
		echo 'Waited 1h for this trace to run. Something must be wrong. Aborting!'
		qdel -u root
		DONE_WAIT=true
	fi
done
vw-log-plot-cachehitmiss -d /var/workspace//graphs/SmallTraceHitMiss -o png LFU-1 LRU-1 LFUGlobal-1 LRUGlobal-1 

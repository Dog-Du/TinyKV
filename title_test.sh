#!/bin/bash

# settings to change
project="2b"
removelog=1     
times=5

# title="TestConfChangeRemoveLeader3B"
# title="TestSplitRecoverManyClients3B"
# title="TestConfChangeRecoverManyClients3B"
# title="TestSplitConfChangeSnapshotUnreliableRecover3B"
# title="TestConfChangeRemoveLeader3B"
# title="TestConfChangeSnapshotUnreliableRecover3B"
# title="TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B"
# title="TestConfChangeUnreliableRecover3B"
title="TestPersistPartition2B"
LOG_LEVEL=debug

# no change below this line
if [ ! -d "./test_output" ]; then
    mkdir "./test_output"
fi

logdir="test_output/$project/$title"
if [ ! -d $logdir ]; then
    mkdir $logdir
fi

lastdir="$logdir/`date +%Y%m%d%H%M%S`"
if [ ! -d $lastdir ]; then
    mkdir $lastdir
fi
summary="$lastdir/summary.log"
echo "times pass fail panic runtime panicinfo" >> $summary

totalpass=0
totalfail=0
totalpanic=0
totalruntime=0

for i in $(seq 1 $times)
do 
    logfile="${lastdir}/$i.log"
    start=$(date +%s)
    echo "start $i times"
    (LOG_LEVEL=${LOG_LEVEL} GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 --timeout=300s ./kv/test_raftstore -run ^TestPersistPartition2B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSplitRecover3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSplitRecoverManyClients3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRecoverManyClients3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecover3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeRemoveLeader3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeSnapshotUnreliableRecover3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B|| true) >> $logfile
    # (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B|| true) >> $logfile

    end=$(date +%s)
    pass_count=$(grep -i "PASS" $logfile | wc -l)
    echo "pass count: $pass_count"
    fail_count=$(grep -i "fail" $logfile | wc -l)
    echo "fail count: $fail_count"
    panic_count=$(grep -i "panic" $logfile | wc -l)
    echo "panic count: $panic_count"
    runtime=$((end-start))

    panic_info=$(grep -m 1 -i "panic" $logfile)

    echo "$i $pass_count $fail_count $panic_count $runtime $panic_info" >> $summary

    totalpass=$((totalpass+pass_count))
    totalfail=$((totalfail+fail_count))
    totalpanic=$((totalpanic+panic_count))
    totalruntime=$((totalruntime+runtime))

    # if pass, remove the log
    if [ $removelog -eq 1 ]; then
        if [ $pass_count -eq 2 ]; then
            rm $logfile
        fi
        sleep 5
    fi
done

echo "total $totalfail $totalpanic $totalruntime" >> $summary

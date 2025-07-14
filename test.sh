#!/bin/bash

# settings to change
times=20
project="3b"
removelog=1

# don't change
if [ ! -d "./test_output" ]; then
    mkdir "./test_output"
fi

logdir="./test_output/${project}"
if [ ! -d $logdir ]; then
    mkdir $logdir
fi
lastdir="${logdir}/`date +%Y%m%d%H%M%S`"
if [ ! -d $lastdir ]; then
    mkdir $lastdir
fi

statistics=0
totalpass=0
totalfail=0
totalpanic=0
totalruntime=0
LOG_LEVEL=fatal

summary="${lastdir}/summary.log"
echo "times.   pass.       fail.      panic.       error.   runtime." >> $summary

for i in $(seq 1 $times)
do
    logfile="${lastdir}/$i.log"
    start=$(date +%Y-%m-%d" "%H:%M:%S)
    start_s=$(date +%s)
    echo "make project${project} $i times"

    echo "$start begin to test" >> $logfile 
    LOG_LEVEL=${LOG_LEVEL} make project${project} >> $logfile
    end=$(date +%Y-%m-%d" "%H:%M:%S)
    end_s=$(date +%s)
    runtime=$((end_s-start_s))
    echo "$start begins, $end ends, total:$runtime" >> $logfile
    echo "$start begins, $end ends, total:$runtime"

    panic_count=0
    if [ $statistics -eq 1 ]; then
        fail_count=$(grep -i "fail" $logfile | wc -l)
        echo "fail count: $fail_count"
        panic_count=$(grep -i "panic" $logfile | wc -l)
        echo "panic count: $panic_count"
        error_count=$(grep -i "error" $logfile | wc -l)
        echo "error count: $error_count"
    fi

    pass_count=$(grep -i "PASS" $logfile | wc -l)
    echo "pass count: $pass_count"
    echo "$i.   $pass_count.    $fail_count.   $panic_count.   $error_count.   $runtime." >> $summary
    totalpass=$((totalpass+pass_count))
    totalfail=$((totalfail+fail_count))
    totalpanic=$((totalpanic+panic_count))
    totalruntime=$((totalruntime+runtime))

    # if pass, remove the log
    if [ $removelog -eq 1 ]; then
        if [ $panic_count -lt 0 ]; then
            rm $logfile
        fi
        sleep 1
    fi
done

echo "total $totalfail $totalpanic $totalruntime" >> $summary

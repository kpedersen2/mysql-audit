#/bin/bash

while getopts n:t: flag
do
    case "${flag}" in
        n) num_loops=${OPTARG};;
        t) threads=${OPTARG};;
    esac
done

echo "num loops: $num_loops";
echo "threads: $threads";

for i in $(seq $threads); do
    ~/sandboxes/msb_5_7_31/use -vvv -N audit_load_test -e "call load_proc($num_loops);" &
done

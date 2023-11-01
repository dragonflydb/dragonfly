#!/bin/sh

rm ./keys_*.txt
for i in `seq 1 $1`
do
    echo "launching process $i to fill.."
    ./fill_db.py -f &
done

wait

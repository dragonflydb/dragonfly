#!/bin/bash

while true; do
./dragonfly  --vmodule=accept_server=1,listener_interface=1 --logbuflevel=-1 &
DRAGON_PID=$!
echo "dragonfly pid $DRAGON_PID"

sleep 0.5

memtier_benchmark -p 6379 --ratio 1:0  -n 100000 --threads=2 --expiry-range=15-25  --distinct-client-seed \
                  --hide-histogram 2> /dev/null > /dev/null &
MEMT_ID=$!

echo "memtier pid $MEMT_ID"
echo "Running.............."
sleep 5
echo "killing dragonfly"

kill $DRAGON_PID
wait $DRAGON_PID

done
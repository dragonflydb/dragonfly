#!/bin/bash

memtier_benchmark -p 6379 --command "sadd __key__ __data__"   -n 20 --threads=4 \
    -c 10 --command-key-pattern=R --distinct-client-seed -c 30 --data-size=64 \
    --key-prefix="key:"  --hide-histogram --random-data --key-maximum=10000


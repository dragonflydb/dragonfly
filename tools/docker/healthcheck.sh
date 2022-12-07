#!/bin/bash

HOST="localhost"
PORT=6379

echo "ping" | nc -q1 $HOST $PORT

exit $?

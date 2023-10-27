#!/bin/sh
ps -ef | grep fill_db.py | grep -v grep | awk '{print $2}' | xargs kill -9

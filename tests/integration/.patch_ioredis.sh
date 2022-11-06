#!/usr/bin/env bash

# This script is only temporary until issue https://github.com/luin/ioredis/issues/1671 is resolved (hopefully soon)
# what we are doing here, is making sure that we would not compare the result case sensitive.
sed -i 's/expect(args\[0\]).to.eql("get")/expect(args\[0\]).to.match(\/get\/i)/' $1
sed -i 's/args\[0\] === "set"/args\[0\] === "set" || args\[0\] === "SET"/' $1

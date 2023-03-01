#!/usr/bin/env bash
# Running this with --build would build the image as well
if [ "$1" = "--build" ]; then
    docker build -t ioredis-test -f ./ioredis.Dockerfile . || {
        echo "failed to build io redis image"
        exit 1
    }
fi

# run the tests
echo "running ioredis tests"
docker run --rm -i --network=host ioredis-test ./run_tests.sh
if [ $? -ne 0 ];then
	echo "some tests failed - please look at the output from this run"
	exit 1
else
	echo "finish runing tests successfully"
	exit 0
fi

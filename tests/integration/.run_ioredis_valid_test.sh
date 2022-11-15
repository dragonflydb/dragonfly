#!/usr/bin/env bash

# The following tests are not supported
#"should reconnect if reconnectOnError
# supported in transaction blocks
# rejects when monitor is disabled
# should resend unfulfilled commands to the correct
# should set the name before any subscribe
# should name the connection if options
# scanStream
# should affect the old way
# should support Map
# should support object
# should batch all commands before ready event
# should support key prefixing for sort
# should be sent on the connect event

## Some issues that are still open need to be resolved such as
# https://github.com/dragonflydb/dragonfly/issues/457
# and https://github.com/dragonflydb/dragonfly/issues/458


# The follwing tests would pass once we support script flush command:
# does not fallback to EVAL in manual transaction
# does not fallback to EVAL in regular
# should reload scripts on redis restart (reconnect)"


TS_NODE_TRANSPILE_ONLY=true NODE_ENV=test mocha \
"test/helpers/*.ts" "test/unit/**/*.ts" "test/functional/**/*.ts" \
-g "should reload scripts on redis restart|should reconnect if reconnectOnError|should be supported in transaction blocks|rejects when monitor is disabled|should resend unfulfilled commands to the correct|should set the name before any subscribe|should name the connection if options|scanStream|does not fallback to EVAL|should try to use EVALSHA and fallback to EVAL|should use evalsha when script|should affect the old way|should support Map|should support object|should batch all commands before ready event|should support key prefixing for sort|should be sent on the connect event|spub|ssub|should support parallel script execution|works for moved" \
--invert

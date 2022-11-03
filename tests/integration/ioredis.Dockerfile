# syntax=docker/dockerfile:1

FROM node:18.7.0
ENV NODE_ENV=development
ENV RUN_IN_DOCKER=1

WORKDIR /app

# Git
RUN apt update -y && apt install -y git

# Clone ioredis v5.2.3
RUN git clone --branch v5.2.3 https://github.com/luin/ioredis

WORKDIR /app/ioredis

RUN npm install

# This is required until https://github.com/luin/ioredis/issues/1671 is resolved.
ADD .patch_ioredis.sh patch_ioredis.sh

# Script to run the tests that curretly pass successfully.
# Note that in DF we still don't have support for cluster and we
# want to skip tests such as elasticache, also we have some issues that
# need to be resolved such as
# https://github.com/dragonflydb/dragonfly/issues/457
# and https://github.com/dragonflydb/dragonfly/issues/458
ADD .run_ioredis_valid_test.sh run_tests.sh

# this would enable running monitor commands successfully
# we may need to remove this incase we have other fix
RUN ./patch_ioredis.sh test/functional/monitor.ts

ENTRYPOINT [ "npm", "run", "env", "--", "TS_NODE_TRANSPILE_ONLY=true", "NODE_ENV=test" ]

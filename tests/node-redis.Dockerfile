# syntax=docker/dockerfile:1

FROM node:18.7.0
ENV NODE_ENV=development

WORKDIR /app
# Clone node-redis dragonfly fork
RUN git clone -b dragonfly https://github.com/dragonflydb/node-redis.git

WORKDIR /app/node-redis

RUN npm install

RUN npm run build -w ./packages/client

RUN npm run build -w ./packages/test-utils

CMD npm run test -w ./packages/client -- --redis-version=2.8 

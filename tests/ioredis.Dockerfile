# syntax=docker/dockerfile:1

FROM node:18.7.0
ENV NODE_ENV=development

WORKDIR /app

# Git 
RUN apt -y install git 

# Clone ioredis v5.2.3
RUN git clone --branch v5.2.3 https://github.com/luin/ioredis

WORKDIR /app/ioredis

RUN npm install

ENTRYPOINT [ "npm", "run", "env", "--", "TS_NODE_TRANSPILE_ONLY=true", "NODE_ENV=test" ]

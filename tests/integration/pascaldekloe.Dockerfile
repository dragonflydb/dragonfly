FROM golang:1.20

RUN git clone https://github.com/pascaldekloe/redis.git
WORKDIR redis

ENV TEST_REDIS_ADDR=localhost

CMD ["go", "test", "-v"]

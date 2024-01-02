# to build the test
# docker build --pull -t relay-test -f ./relay.Dockerfile .
# to run the test, start dragonfly locally with port 6379
# then
# docker run --network=host -t relay-test

FROM linuxmintd/mint21.2-amd64

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update

RUN add-apt-repository -y ppa:ondrej/php

RUN apt-get install -y \
  curl \
  php-dev

# Install Relay dependencies
RUN apt-get install -y \
  php-msgpack \
  php-igbinary

ARG RELAY=v0.6.8

# Download Relay
RUN PHP=$(php -r 'echo substr(PHP_VERSION, 0, 3);') \
  && curl -L "https://builds.r2.relay.so/$RELAY/relay-$RELAY-php$PHP-debian-x86-64+libssl3.tar.gz" | tar xz --strip-components=1 -C /tmp

# Copy relay.{so,ini}
RUN cp "/tmp/relay.ini" $(php-config --ini-dir)/30-relay.ini \
  && cp "/tmp/relay-pkg.so" $(php-config --extension-dir)/relay.so

# Inject UUID
RUN sed -i "s/00000000-0000-0000-0000-000000000000/$(cat /proc/sys/kernel/random/uuid)/" $(php-config --extension-dir)/relay.so

# needed by the Relay benchmark
RUN apt-get install -y composer php-curl

# checkout relay benchmark
RUN git clone https://github.com/cachewerk/relay.git

WORKDIR relay
RUN composer install

WORKDIR benchmarks

CMD ./run --filter '^(Relay)'

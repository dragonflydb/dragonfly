# Build DragonflyDB From Source

## Running the server

Dragonfly runs on linux. We advice running it on linux version 5.11 or later
but you can also run Dragonfly on older kernels as well.

### WARNING: Building from source on older kernels WILL NOT WORK.

## Step 1

```bash
git clone --recursive https://github.com/dragonflydb/dragonfly && cd dragonfly
```

## Step 2
```bash
# Install dependencies
sudo apt install ninja-build libunwind-dev libboost-fiber-dev libssl-dev \
     autoconf-archive libtool cmake g++
```

## Step 3

```bash
# Configure the build
./helio/blaze.sh -release

# Build
cd build-opt && ninja dragonfly

```

## Step 4
```bash
# Run
./dragonfly --alsologtostderr

```

Dragonfly DB will answer to both `http` and `redis` requests out of the box!

You can use `redis-cli` to connect to `localhost:6379` or open a browser and visit `http://localhost:6379`

## Step 5

Connect with a redis client

```bash
redis-cli
127.0.0.1:6379> set hello world
OK
127.0.0.1:6379> keys *
1) "hello"
127.0.0.1:6379> get hello
"world"
127.0.0.1:6379>
```

## Step 6

Continue being great and build your app with the power of DragonflyDB!

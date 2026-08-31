# Build DragonflyDB From Source

## Running the server

Dragonfly runs on linux. We advise running it on linux version 5.11 or later
but you can also run Dragonfly on older kernels as well.

## Step 1 - install dependencies

On Debian/Ubuntu:

```bash
sudo apt install ninja-build libunwind-dev libboost-context-dev libssl-dev \
     autoconf-archive libtool cmake g++ bison  zlib1g-dev
```

On Fedora:

```bash
sudo dnf install -y automake boost-devel g++ git cmake libtool ninja-build \
     openssl-devel libunwind-devel autoconf-archive patch bison libstdc++-static
```

On openSUSE:

```bash
sudo zypper install automake boost-devel gcc-c++ git cmake libtool ninja \
     openssl-devel libunwind-devel autoconf-archive patch bison \
     libboost_context-devel libboost_system-devel
```

On FreeBSD:

```bash
pkg install -y git bash cmake ninja libunwind boost-libs autoconf automake libtool gmake bison
```

## Step 2 - clone the project

```bash
git clone --recursive https://github.com/dragonflydb/dragonfly && cd dragonfly
```

## Step 3 - configure & build it

```bash
# Configure the build
./helio/blaze.sh -release

# Build
cd build-opt && ninja dragonfly

```

### Build options

| Option               | Description                                                                                                                                                                                                                              |
| ---------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| WITH_AWS             | Include AWS client. Required for cloud snapshots                                                                                                                                                                                         |
| WITH_GCP | Include GCP client. Required for cloud snapshots |
| WITH_GPERF | Include gperf tracing profiler |
| WITH_TIERING | Include tiered storage module |
| WITH_SEARCH          | Include Search module                                                                                                                                                                                                                    |
| WITH_COLLECTION_CMDS | Include commands for collections (SET, HSET, ZSET)                                                                                                                                                                                       |
| WITH_EXTENSION_CMDS  | Include extension commands (Bloom, HLL, JSON, ...)                                                                                                                                                                                       |
| USE_MOLD             | Uses the mold linker to reduce link time overhead while enabling Link Time Optimization (LTO) for improved runtime performance. Recommended for benchmarking and production. |
| ENABLE_CCACHE | Use ccache as the compiler launcher when it is installed (ON by default). Disable with -DENABLE_CCACHE=OFF |

Minimal debug build:

```bash
./helio/blaze.sh -DWITH_GPERF=OFF -DWITH_AWS=OFF -DWITH_GCP=OFF -DWITH_TIERING=OFF -DWITH_SEARCH=OFF -DWITH_COLLECTION_CMDS=OFF -DWITH_EXTENSION_CMDS=OFF
```

### ccache

For **local builds**, ccache is enabled automatically whenever the `ccache` binary is on your `PATH`, so incremental rebuilds recompile only the files you changed. (In CI it is enabled per-workflow, and the production `make release` build forces it off.) If you do **not** want ccache, disable it in any of these ways:

- configure with `-DENABLE_CCACHE=OFF` (e.g. `./helio/blaze.sh -DENABLE_CCACHE=OFF`)
- set the `CCACHE_DISABLE=1` environment variable
- set `disable = true` in your ccache config (`ccache -o disable=true`)

#### Cache size

Dragonfly is large, so ccache's default size (5 GB, or ~5% of disk on ccache ≥ 4.10) fills up quickly - more so with several build configs or multiple worktrees — and hit rates drop once it's full. The right size depends on your free disk space and how many projects share the (per-user) cache, so it's left to you. If rebuilds feel slow, raise it (50 GB is a comfortable value):

```bash
ccache -M 50G              # per-user, global; applies to all projects
ccache -p | grep max_size  # verify
```

## Step 4 - voilà

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

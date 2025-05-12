# Fuzzing Dragonfly with AFL++

This directory contains tools for fuzzing Dragonfly using American Fuzzy Lop (AFL++), which helps find bugs and vulnerabilities through the Redis protocol.

## Requirements

1. Python 3.6 or higher
2. AFL++ (installed and configured)
3. Dragonfly (server running on Redis port)
4. Network access to the Dragonfly server

## Directory Structure

- `redis_fuzzer.py` - Python script for fuzzing Redis commands
- `run_afl_fuzzing.sh` - Bash script to run AFL++ with various strategies
- `redis.dict` - Dictionary of Redis commands and typical values (generated automatically)
- `input/` - Directory with initial input data
- `output/` - Directory for fuzzing results

## How to Use

### 1. Preparation

Make sure Dragonfly is running and accessible via Redis port:

```bash
# Check if Dragonfly server is running
nc -z 127.0.0.1 6379 || echo "Server not running"
```

### 2. Running the Fuzzer

Run the `run_afl_fuzzing.sh` script:

```bash
cd /path/to/dragonfly
chmod +x tests/afl/run_afl_fuzzing.sh
./tests/afl/run_afl_fuzzing.sh
```

By default, the script will use the following settings:
- Redis host: 127.0.0.1
- Redis port: 6379
- Output directory: tests/afl/output
- Input directory: tests/afl/input
- Dictionary: tests/afl/redis.dict

You can change these settings by specifying the appropriate environment variables:

```bash
REDIS_HOST=192.168.1.100 REDIS_PORT=6380 ./tests/afl/run_afl_fuzzing.sh
```

### 3. Monitoring the Fuzzing Process

To view the current fuzzing status, use:

```bash
afl-whatsup tests/afl/output
```

For detailed statistics viewing:

```bash
afl-plot tests/afl/output/default /path/to/report
```

### 4. Analyzing Results

After the fuzzing is complete (or after a forced stop), results will be available in the `tests/afl/output` directory.

The `final_report.txt` file contains a summary report of the fuzzing results.

Files that caused errors will be located in the directories:
- `output/*/crashes/` - for critical errors
- `output/*/hangs/` - for hanging sessions

### 5. Manual Testing of Discovered Issues

To manually reproduce found issues, use:

```bash
cat tests/afl/output/default/crashes/id:000000* | python3 tests/afl/redis_fuzzer.py --reproduce
```

## Fuzzing Strategies

The simplified version of the script runs AFL++ in non-instrumented mode, which is suitable for testing network applications like Dragonfly.

## Extending the Dictionary

If you want to add new commands or data types to the dictionary, edit the `redis_fuzzer.py` file and run:

```bash
python3 tests/afl/redis_fuzzer.py --create-dict
```

## Troubleshooting

### Problem: "Dragonfly is not running"

Make sure that Dragonfly or Redis server is running at the specified address and port.

### Problem: "afl-fuzz: command not found"

Make sure AFL++ is installed and available in the system PATH.

### Problem: "No instrumentation detected"

When using Python with AFL++, this warning may occur. It's normal for network testing and we're using the `-n` option to run in non-instrumented mode.

## Debugging

For debugging, you can run the script without AFL++:

```bash
python3 tests/afl/redis_fuzzer.py
```

This will create a random set of commands and execute them against the server.

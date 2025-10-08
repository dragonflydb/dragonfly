# How to Reproduce AFL++ Crashes

## Quick Method

### 1. Use reproduce_crash.sh script

```bash
# Automatically shows hex, commands, logs and stack trace
./fuzz/reproduce_crash.sh fuzz/artifacts/resp/fuzzer01/crashes/id:000000*
```

## Manual Method

### 1. Find crashes

```bash
ls -lh fuzz/artifacts/resp/fuzzer01/crashes/
```

File name shows:
- `sig:06` - SIGABRT (assertion failure)
- `sig:11` - SIGSEGV (segfault)
- `time:2418` - when found (ms)
- `execs:3493` - after how many executions

### 2. View input (hex)

```bash
od -A x -t x1z fuzz/artifacts/resp/fuzzer01/crashes/id:000000*
```

### 3. Reproduce with monitor

```bash
# See which commands were executed
./build-fuzz/fuzz/resp_fuzz \
  --fuzzer_monitor=true \
  --fuzzer_monitor_file=crash_commands.log \
  < fuzz/artifacts/resp/fuzzer01/crashes/id:000000*

# View commands
cat crash_commands.log
```

### 4. Reproduce with GDB

```bash
# Run with GDB
gdb --args ./build-fuzz/fuzz/resp_fuzz \
  --fuzzer_monitor=true \
  --fuzzer_monitor_file=debug.log

# In GDB:
(gdb) run < fuzz/artifacts/resp/fuzzer01/crashes/id:000000*
(gdb) bt          # Stack trace
(gdb) info locals # Local variables
(gdb) frame 0     # Top frame details
```

### 5. Reproduce with Address Sanitizer

```bash
# Rebuild with ASAN
cd fuzz
CC=afl-clang-fast CXX=afl-clang-fast++ \
  cmake -DDFLY_FUZZ=ON -DWITH_ASAN=ON -B ../build-asan
cd ../build-asan
ninja resp_fuzz

# Run with ASAN
./fuzz/resp_fuzz < ../fuzz/artifacts/resp/fuzzer01/crashes/id:000000*
```

ASAN will show:
- Use-after-free
- Heap buffer overflow
- Stack buffer overflow
- Memory leaks

## Crash Example

```
Crash file: id:000000,sig:06,src:000001,time:2418,execs:3493,op:havoc,rep:5
Size: 79 bytes

=== Hex dump ===
000000 2a 32 0d 0a 24 33 33 33 33 33 33 33 33 33 33 33  >*2..$33333333333<
000010 0d 0a 47 45 54 0d 33 33 33 33 33 33 33 33 33 0d  >..GET.333333333.<
000020 0a 47 02 00 0d 0a 33 33 33 33 33 33 33 33 33 33  >.G....3333333333<

=== Commands ===
[conn2] "ET" "333333333"
[conn2] "G"
[conn3] "33333333333"
[conn4] "GET"

=== Stack trace ===
#0  abort() at libc.so.6
#1  google::LogMessage::Fail()
#2  dfly::Service::DispatchCommand()
#3  FuzzerState::ProcessConnectionInput()
```

## Crash Minimization

AFL++ can reduce crash input size:

```bash
# Minimize input (keep only essential bytes)
afl-tmin \
  -i fuzz/artifacts/resp/fuzzer01/crashes/id:000000* \
  -o minimized_crash.bin \
  ./build-fuzz/fuzz/resp_fuzz

# Now minimized_crash.bin is the smallest input that triggers crash
od -A x -t x1z minimized_crash.bin
```

## Bug Reporting

After reproducing crash, create an issue with:

1. **Minimized input** (hex dump)
2. **Stack trace** (from GDB or ASAN)
3. **Commands** (from monitor log)
4. **Build info**: IoUring/Epoll, Debug/Release
5. **Reproduction steps**

Example:
```markdown
## Bug: SIGABRT in DispatchCommand

### Input (79 bytes)
2a 32 0d 0a 24 33 33 33 33 33 33 33 33 33 33 33
0d 0a 47 45 54 0d 33 33 33 33 33 33 33 33 33 0d
...

### Commands executed:
[conn2] "ET" "333333333"
[conn2] "G"

### Stack trace:
#0  abort()
#1  google::LogMessage::Fail()
#2  dfly::Service::DispatchCommand()

### Reproduction:
./build-fuzz/fuzz/resp_fuzz < crash_input.bin
```

## Hangs (Timeouts)

Found hangs/infinite loops:

```bash
ls -lh fuzz/artifacts/resp/fuzzer01/hangs/

# Reproduce hang (with timeout)
timeout 5 ./build-fuzz/fuzz/resp_fuzz \
  --fuzzer_monitor=true \
  --fuzzer_monitor_file=hang_commands.log \
  < fuzz/artifacts/resp/fuzzer01/hangs/id:000000*

# See what hung
cat hang_commands.log
```

## Useful Commands

```bash
# How many crashes?
ls fuzz/artifacts/resp/fuzzer01/crashes/id:* 2>/dev/null | wc -l

# How many hangs?
ls fuzz/artifacts/resp/fuzzer01/hangs/id:* 2>/dev/null | wc -l

# What signals?
ls fuzz/artifacts/resp/fuzzer01/crashes/ | grep -o "sig:[0-9]*" | sort | uniq -c

# Latest crash
ls -t fuzz/artifacts/resp/fuzzer01/crashes/id:* 2>/dev/null | head -1
```

## State-Dependent Crashes

Some crashes only reproduce under AFL++ (persistent mode):

**Why?**
- Crash happens after many iterations
- State accumulation (memory not cleaned)
- Race conditions (timing-dependent)

**How to debug:**
1. Run long fuzzing session
2. Check Dragonfly ERROR logs in `artifacts/resp/`
3. Look for patterns before crash
4. May need to run under AFL++ to reproduce

## Advanced Debugging

### With Valgrind

```bash
valgrind --leak-check=full ./build-fuzz/fuzz/resp_fuzz < crash_file
```

### With RR (Record & Replay)

```bash
# Record execution
rr record ./build-fuzz/fuzz/resp_fuzz < crash_file

# Replay and debug
rr replay
(gdb) continue
(gdb) reverse-continue  # Go back in time!
```

### Check Dragonfly Logs

```bash
# View errors that happened during fuzzing
cat fuzz/artifacts/resp/dragonfly*.ERROR

# View warnings
cat fuzz/artifacts/resp/dragonfly*.WARNING

# Search for specific error
grep -r "assertion\|FATAL\|Check failed" fuzz/artifacts/resp/
```

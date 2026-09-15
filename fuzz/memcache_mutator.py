"""AFL++ custom mutator for memcache text protocol.

Mutates at the command level instead of random bytes,
keeping memcache protocol framing valid.

Usage:
    export PYTHONPATH=/path/to/dragonfly/fuzz
    export AFL_PYTHON_MODULE=memcache_mutator
    afl-fuzz ...
"""

import random

# fmt: off
# (command, type, min_extra_args, max_extra_args)
# type: "store" = key flags exptime bytes [noreply]\r\ndata\r\n
#       "cas"   = key flags exptime bytes cas_unique [noreply]\r\ndata\r\n
#       "get"   = key [key ...]\r\n
#       "gat"   = exptime key [key ...]\r\n
#       "delta" = key delta [noreply]\r\n
#       "del"   = key [noreply]\r\n
#       "bare"  = \r\n (no args)
#       "meta_store" = key datalen [flags...]\r\ndata\r\n
#       "meta"  = key [flags...]\r\n

COMMANDS = [
    # Store commands
    ("set",     "store"),
    ("add",     "store"),
    ("replace", "store"),
    ("append",  "store"),
    ("prepend", "store"),
    # cas dispatch is dead (CLIENT_ERROR) but exercises ParseStore's CAS branch.
    ("cas",     "cas"),
    # Retrieval
    ("get",     "get"),
    ("gets",    "get"),
    ("gat",     "gat"),
    ("gats",    "gat"),
    # Delete / arithmetic
    ("delete",  "del"),
    ("incr",    "delta"),
    ("decr",    "delta"),
    # Utility. flush_all omitted: maps to restricted FLUSHDB (always SERVER_ERROR).
    ("stats",     "bare"),
    ("version",   "bare"),
    ("quit",      "bare"),
    # Meta. mn/me omitted: unconditionally rejected by parser/dispatch.
    ("ms",      "meta_store"),
    ("mg",      "meta"),
    ("md",      "meta"),
    ("ma",      "meta_arithm"),
]
# fmt: on

KEYS = [b"k", b"key", b"k1", b"k2", b"k3", b"mykey", b"counter", b"buf"]
VALUES = [b"abc", b"hello", b"x", b"", b"0", b"12345", b"\x00\xff", b"a" * 100]
EXPIRY = [b"0", b"10", b"100", b"3600", b"9999999"]
FLAGS = [b"0", b"1", b"255", b"65535", b"4294967295"]
DELTAS = [b"1", b"5", b"10", b"100", b"0", b"99999999999"]
# Only flags ParseMeta accepts; anything else is PARSE_ERROR.
META_FLAGS = [b"T30", b"T0", b"F7", b"v", b"h", b"l", b"t", b"c", b"f", b"q"]
MS_MODES = [b"MS", b"ME", b"MA", b"MR", b"MP"]  # ms mode flags
MA_FLAGS = [b"D5", b"D10", b"MI", b"MD", b"T30", b"q", b"v"]  # ma (meta-arithmetic)
FUZZ_VALUES = [b"\x00", b"\xff" * 4, b"\r\n", b"A" * 256, b"-1", b"NaN"]


def init(seed):
    random.seed(seed)


def _random_key():
    if random.random() < 0.8:
        return random.choice(KEYS)
    return random.choice(FUZZ_VALUES)


def _random_value():
    if random.random() < 0.7:
        return random.choice(VALUES)
    return random.choice(FUZZ_VALUES)


def _random_command():
    """Generate a single random memcache command."""
    cmd_name, cmd_type = random.choice(COMMANDS)
    cmd = cmd_name.encode() if isinstance(cmd_name, str) else cmd_name

    if cmd_type == "store":
        key = _random_key()
        flags = random.choice(FLAGS)
        expiry = random.choice(EXPIRY)
        value = _random_value()
        noreply = b" noreply" if random.random() < 0.3 else b""
        return (
            cmd
            + b" "
            + key
            + b" "
            + flags
            + b" "
            + expiry
            + b" "
            + str(len(value)).encode()
            + noreply
            + b"\r\n"
            + value
            + b"\r\n"
        )

    elif cmd_type == "cas":
        key = _random_key()
        flags = random.choice(FLAGS)
        expiry = random.choice(EXPIRY)
        value = _random_value()
        cas_id = str(random.randint(0, 99999)).encode()
        noreply = b" noreply" if random.random() < 0.3 else b""
        return (
            cmd
            + b" "
            + key
            + b" "
            + flags
            + b" "
            + expiry
            + b" "
            + str(len(value)).encode()
            + b" "
            + cas_id
            + noreply
            + b"\r\n"
            + value
            + b"\r\n"
        )

    elif cmd_type == "get":
        nkeys = random.randint(1, 4)
        keys = b" ".join(_random_key() for _ in range(nkeys))
        return cmd + b" " + keys + b"\r\n"

    elif cmd_type == "gat":
        expiry = random.choice(EXPIRY)
        nkeys = random.randint(1, 3)
        keys = b" ".join(_random_key() for _ in range(nkeys))
        return cmd + b" " + expiry + b" " + keys + b"\r\n"

    elif cmd_type == "delta":
        key = _random_key()
        delta = random.choice(DELTAS)
        noreply = b" noreply" if random.random() < 0.3 else b""
        return cmd + b" " + key + b" " + delta + noreply + b"\r\n"

    elif cmd_type == "del":
        key = _random_key()
        noreply = b" noreply" if random.random() < 0.3 else b""
        return cmd + b" " + key + noreply + b"\r\n"

    elif cmd_type == "meta_store":
        key = _random_key()
        value = _random_value()
        flags = random.sample(META_FLAGS, random.randint(0, 3))
        if random.random() < 0.4:
            flags.append(random.choice(MS_MODES))
        extra = (b" " + b" ".join(flags)) if flags else b""
        return (
            cmd + b" " + key + b" " + str(len(value)).encode() + extra + b"\r\n" + value + b"\r\n"
        )

    elif cmd_type == "meta_arithm":
        key = _random_key()
        flags = random.sample(MA_FLAGS, random.randint(0, 2))
        extra = (b" " + b" ".join(flags)) if flags else b""
        return cmd + b" " + key + extra + b"\r\n"

    elif cmd_type == "meta":
        key = _random_key()
        meta_flags = b" ".join(random.sample(META_FLAGS, random.randint(0, 3)))
        extra = (b" " + meta_flags) if meta_flags else b""
        return cmd + b" " + key + extra + b"\r\n"

    else:  # bare
        return cmd + b"\r\n"


def _parse_mc_commands(buf, strict=False):
    """Parse memcache text protocol into (commands, success), each command a
    (line, data_block) pair.

    strict=False is the fuzzing hot path (best-effort). strict=True is the CI
    gate: a store/ms data block that is missing, truncated, or not CRLF-
    terminated is a hard failure and the whole buffer must be consumed, matching
    what the server accepts."""
    commands = []
    data = bytes(buf)
    pos = 0

    STORE = (b"set", b"add", b"replace", b"append", b"prepend", b"cas")

    while pos < len(data):
        end = data.find(b"\r\n", pos)
        if end < 0:
            if strict:
                return ([], False)
            break

        line = data[pos:end]
        pos = end + 2

        parts = line.split(b" ")
        name = parts[0].lower() if parts else b""
        # Byte-count field: index 4 for classic stores/cas, index 2 for meta-set.
        nbytes_idx = None
        if len(parts) >= 5 and name in STORE:
            nbytes_idx = 4
        elif len(parts) >= 3 and name == b"ms":
            nbytes_idx = 2

        if nbytes_idx is not None:
            try:
                nbytes = int(parts[nbytes_idx])
            except (ValueError, IndexError):
                if strict:
                    return ([], False)
                commands.append((line, None))
                continue
            if pos + nbytes + 2 <= len(data) and data[pos + nbytes : pos + nbytes + 2] == b"\r\n":
                value = data[pos : pos + nbytes]
                pos += nbytes + 2
                commands.append((line, value))
                continue
            if strict:
                return ([], False)

        commands.append((line, None))

    if strict:
        return (commands, len(commands) > 0 and pos == len(data))
    return (commands, len(commands) > 0)


def _commands_to_bytes(commands):
    """Serialize parsed commands back to memcache protocol bytes."""
    parts = []
    for line, value in commands:
        parts.append(line + b"\r\n")
        if value is not None:
            parts.append(value + b"\r\n")
    return b"".join(parts)


def _mutate_commands(commands):
    """Apply random mutations to parsed memcache commands."""
    result = list(commands)

    mutation = random.random()

    if mutation < 0.25 and len(result) > 0:
        # Replace a command entirely
        idx = random.randint(0, len(result) - 1)
        new_cmd = _random_command()
        # Parse the generated command back
        parsed, _ = _parse_mc_commands(new_cmd)
        if parsed:
            result[idx] = parsed[0]

    elif mutation < 0.45 and len(result) > 0:
        # Mutate a key or value in a command
        idx = random.randint(0, len(result) - 1)
        line, value = result[idx]
        parts = line.split(b" ")
        if len(parts) >= 2:
            cmd = parts[0].lower()
            # Mutate the correct key index depending on command
            if cmd in (b"gat", b"gats") and len(parts) >= 3:
                key_idx = random.randint(2, len(parts) - 1)
                parts[key_idx] = _random_key()
            else:
                parts[1] = _random_key()
            if value is not None:
                new_value = _random_value()
                # Update byte count in the header
                length_idx = None
                if cmd == b"ms" and len(parts) >= 3:
                    length_idx = 2
                elif len(parts) >= 5:
                    length_idx = 4
                if length_idx is not None:
                    try:
                        int(parts[length_idx])
                        parts[length_idx] = str(len(new_value)).encode()
                    except ValueError:
                        pass
                value = new_value
            result[idx] = (b" ".join(parts), value)

    elif mutation < 0.6:
        # Insert a new random command
        new_cmd = _random_command()
        parsed, _ = _parse_mc_commands(new_cmd)
        if parsed:
            pos = random.randint(0, len(result))
            result.insert(pos, parsed[0])

    elif mutation < 0.7 and len(result) > 1:
        # Remove a command
        idx = random.randint(0, len(result) - 1)
        result.pop(idx)

    elif mutation < 0.8 and len(result) >= 2:
        # Swap two commands
        i, j = random.sample(range(len(result)), 2)
        result[i], result[j] = result[j], result[i]

    elif mutation < 0.9 and len(result) > 0:
        # Duplicate a command
        idx = random.randint(0, len(result) - 1)
        result.insert(idx + 1, result[idx])

    else:
        # Toggle noreply on a command
        if len(result) > 0:
            idx = random.randint(0, len(result) - 1)
            line, value = result[idx]
            if line.endswith(b" noreply"):
                line = line[:-8]
            else:
                line = line + b" noreply"
            result[idx] = (line, value)

    return result


def fuzz(buf, add_buf, max_size):
    """Main mutation function called by AFL++."""
    commands, ok = _parse_mc_commands(buf)

    if ok and commands:
        mutated = _mutate_commands(commands)
        result = _commands_to_bytes(mutated)
    else:
        n = random.randint(1, 5)
        result = b"".join(_random_command() for _ in range(n))

    if len(result) > max_size:
        result = result[:max_size]
    return bytearray(result)


def havoc_mutation(buf, max_size):
    """Called during havoc stage."""
    commands, ok = _parse_mc_commands(buf)
    if not ok or not commands:
        return bytearray(_random_command()[:max_size])

    mutated = _mutate_commands(commands)
    result = _commands_to_bytes(mutated)
    if len(result) > max_size:
        result = result[:max_size]
    return bytearray(result)


def havoc_mutation_probability():
    return 50

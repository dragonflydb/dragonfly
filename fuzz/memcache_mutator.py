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
    # Utility
    ("flush_all", "bare"),
    ("stats",     "bare"),
    ("version",   "bare"),
    ("quit",      "bare"),
    # Meta commands
    ("ms",      "meta_store"),
    ("mg",      "meta"),
    ("md",      "meta"),
    ("ma",      "meta"),
    ("mn",      "bare"),
    ("me",      "meta"),
]
# fmt: on

KEYS = [b"k", b"key", b"k1", b"k2", b"k3", b"mykey", b"counter", b"buf"]
VALUES = [b"abc", b"hello", b"x", b"", b"0", b"12345", b"\x00\xff", b"a" * 100]
EXPIRY = [b"0", b"10", b"100", b"3600", b"9999999"]
FLAGS = [b"0", b"1", b"255", b"65535", b"4294967295"]
DELTAS = [b"1", b"5", b"10", b"100", b"0", b"99999999999"]
META_FLAGS = [b"T30", b"N10", b"R", b"v", b"h", b"l", b"t", b"c", b"f1", b"q", b"k"]
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
        meta_flags = b" ".join(random.sample(META_FLAGS, random.randint(0, 3)))
        extra = (b" " + meta_flags) if meta_flags else b""
        return (
            cmd + b" " + key + b" " + str(len(value)).encode() + extra + b"\r\n" + value + b"\r\n"
        )

    elif cmd_type == "meta":
        key = _random_key()
        meta_flags = b" ".join(random.sample(META_FLAGS, random.randint(0, 3)))
        extra = (b" " + meta_flags) if meta_flags else b""
        return cmd + b" " + key + extra + b"\r\n"

    else:  # bare
        return cmd + b"\r\n"


def _parse_mc_commands(buf):
    """Best-effort parse of memcache text protocol into list of raw command lines.
    Returns (commands, success) where commands is a list of bytes."""
    commands = []
    data = bytes(buf)
    pos = 0

    while pos < len(data):
        end = data.find(b"\r\n", pos)
        if end < 0:
            break

        line = data[pos:end]
        pos = end + 2

        # Check if this is a store command that has a data block
        parts = line.split(b" ")
        if len(parts) >= 5 and parts[0].lower() in (
            b"set",
            b"add",
            b"replace",
            b"append",
            b"prepend",
            b"cas",
        ):
            try:
                nbytes = int(parts[4])
                if pos + nbytes + 2 <= len(data):
                    value = data[pos : pos + nbytes]
                    pos += nbytes + 2  # skip value + \r\n
                    commands.append((line, value))
                    continue
            except (ValueError, IndexError):
                pass
        elif len(parts) >= 3 and parts[0].lower() == b"ms":
            try:
                nbytes = int(parts[2]) if len(parts) > 2 else int(parts[1])
                if pos + nbytes + 2 <= len(data):
                    value = data[pos : pos + nbytes]
                    pos += nbytes + 2
                    commands.append((line, value))
                    continue
            except (ValueError, IndexError):
                pass

        commands.append((line, None))

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
            parts[1] = _random_key()
            if value is not None:
                new_value = _random_value()
                # Update byte count in the header
                if len(parts) >= 5:
                    try:
                        int(parts[4])
                        parts[4] = str(len(new_value)).encode()
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

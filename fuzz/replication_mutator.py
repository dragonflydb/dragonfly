"""AFL++ custom mutator for Dragonfly master-replica replication fuzzing.

Generates and mutates multiplexed binary inputs understood by
replication_orchestrator.py.

Input format (same as orchestrator):
  byte 0      : N actions
  per action  :
    byte 0    : type  0=MASTER_CMD  1=REPLICA_CMD  2=NET_DROP  3=WAIT
    bytes 1-2 : payload length (uint16 LE)
    bytes 3.. : payload (raw RESP for CMD types, empty otherwise)

Borrows RESP generation helpers from resp_mutator.py.
"""

import random
import struct

from resp_mutator import (
    _commands_to_resp,
    _encode_resp,
    _mutate_commands,
    _parse_resp_commands,
    _pick_command,
    _random_arg,
    _random_command,
)

ACTION_MASTER = 0
ACTION_REPLICA = 1
ACTION_NET_DROP = 2
ACTION_WAIT = 3

# Rough probability for each action type in generated inputs
_ACTION_PROBS = [0.55, 0.20, 0.12, 0.13]

MAX_ACTIONS = 30


# ── Encoding helpers ──────────────────────────────────────────────────────────


def _pack_action(atype: int, payload: bytes = b"") -> bytes:
    return bytes([atype]) + struct.pack("<H", len(payload)) + payload


def _pack_mux(actions: list) -> bytes:
    n = min(len(actions), MAX_ACTIONS)
    return bytes([n]) + b"".join(_pack_action(t, p) for t, p in actions[:n])


def _unpack_mux(data: bytes) -> list:
    if not data:
        return []
    n = min(data[0], MAX_ACTIONS)
    pos = 1
    actions = []
    for _ in range(n):
        if pos + 3 > len(data):
            break
        atype = data[pos]
        plen = struct.unpack_from("<H", data, pos + 1)[0]
        pos += 3
        if atype > 3:
            pos += plen
            continue
        payload = bytes(data[pos : pos + plen])
        pos += plen
        actions.append((atype, payload))
    return actions


# ── Generation ────────────────────────────────────────────────────────────────


def _random_action() -> tuple:
    r = random.random()
    cumulative = 0.0
    atype = ACTION_MASTER
    for t, w in enumerate(_ACTION_PROBS):
        cumulative += w
        if r < cumulative:
            atype = t
            break

    if atype in (ACTION_NET_DROP, ACTION_WAIT):
        return (atype, b"")

    cmd_name, min_args, max_args = _pick_command()
    nargs = random.randint(min_args, max_args)
    payload = _encode_resp(cmd_name, *[_random_arg() for _ in range(nargs)])
    return (atype, payload)


def _generate(n_actions: "int | None" = None) -> bytes:
    if n_actions is None:
        n_actions = random.randint(1, 12)
    return _pack_mux([_random_action() for _ in range(n_actions)])


# ── Mutation ──────────────────────────────────────────────────────────────────


def _mutate(actions: list) -> list:
    if not actions:
        return [_random_action()]

    result = list(actions)
    r = random.random()

    if r < 0.15:
        # Replace one action entirely
        idx = random.randrange(len(result))
        result[idx] = _random_action()

    elif r < 0.28:
        # Insert a new action
        result.insert(random.randint(0, len(result)), _random_action())

    elif r < 0.38 and len(result) > 1:
        # Remove one action
        result.pop(random.randrange(len(result)))

    elif r < 0.48 and len(result) >= 2:
        # Swap two actions
        i, j = random.sample(range(len(result)), 2)
        result[i], result[j] = result[j], result[i]

    elif r < 0.63:
        # Mutate the RESP payload of a CMD action
        cmd_idxs = [i for i, (t, _) in enumerate(result) if t in (ACTION_MASTER, ACTION_REPLICA)]
        if cmd_idxs:
            idx = random.choice(cmd_idxs)
            atype, payload = result[idx]
            cmds, ok = _parse_resp_commands(payload)
            if ok and cmds:
                result[idx] = (atype, _commands_to_resp(_mutate_commands(cmds)))
            else:
                result[idx] = (atype, _random_command())

    elif r < 0.73:
        # Flip master ↔ replica on one CMD action
        cmd_idxs = [i for i, (t, _) in enumerate(result) if t in (ACTION_MASTER, ACTION_REPLICA)]
        if cmd_idxs:
            idx = random.choice(cmd_idxs)
            atype, payload = result[idx]
            result[idx] = (ACTION_REPLICA if atype == ACTION_MASTER else ACTION_MASTER, payload)

    elif r < 0.85:
        # Insert a NET_DROP at a random position
        result.insert(random.randint(0, len(result)), (ACTION_NET_DROP, b""))

    else:
        # Insert a WAIT at a random position
        result.insert(random.randint(0, len(result)), (ACTION_WAIT, b""))

    return result[:MAX_ACTIONS]


# ── AFL++ interface ───────────────────────────────────────────────────────────


def init(seed: int) -> None:
    random.seed(seed)


def fuzz(buf: bytearray, add_buf: bytearray, max_size: int) -> bytearray:
    actions = _unpack_mux(bytes(buf))
    if actions:
        result = _pack_mux(_mutate(actions))
    else:
        result = _generate()
    return bytearray(result[:max_size])


def havoc_mutation(buf: bytearray, max_size: int) -> bytearray:
    actions = _unpack_mux(bytes(buf))
    if not actions:
        return bytearray(_generate()[:max_size])
    return bytearray(_pack_mux(_mutate(actions))[:max_size])


def havoc_mutation_probability() -> int:
    return 40

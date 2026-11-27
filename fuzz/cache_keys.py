"""Key choice shared by the custom mutators in cache-mode runs.

run_fuzzer.sh exports FUZZ_CACHE_FILL, the harness's DEBUG POPULATE arguments
("<count> <prefix> <size>"). Some generated keys then name fill keys, half of them existing and
half new: existing ones are bumped and evicted while in use, and new ones need table growth, so
under memory pressure the insert itself evicts. Bursts of new keys keep that growth going, since
heartbeat eviction frees room all over the table faster than single keys refill it.
"""

import os
import random

_WEIGHT = 0.1
_fill = os.environ.get("FUZZ_CACHE_FILL", "").split()
_count = int(_fill[0]) if _fill and _fill[0].isdigit() else 0

# The prefix defaults to "key", as in DEBUG POPULATE.
_prefix = _fill[1] if len(_fill) > 1 else "key"
_SPACE = (_prefix.encode(), 2 * _count) if _count > 0 else None


def fill_key():
    """Returns a fill key for about 10% of the calls in cache-mode runs, None otherwise."""
    if _SPACE is None or random.random() >= _WEIGHT:
        return None
    prefix, space = _SPACE
    return b"%s:%d" % (prefix, random.randrange(space))


def burst_keys():
    """Returns 4-8 new keys above the fill for about 10% of the calls in cache-mode runs, None
    otherwise."""
    if _SPACE is None or random.random() >= _WEIGHT:
        return None
    prefix, space = _SPACE
    count = random.randint(4, 8)
    return [b"%s:%d" % (prefix, random.randrange(space // 2, space)) for _ in range(count)]

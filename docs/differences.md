# Differences with Redis

## String lengths, indices.

String sizes are limited to 256MB.
Indices (say in GETRANGE and SETRANGE commands) should be signed 32 bit integers in range
[-2147483647, 2147483648].

### String handling.

SORT does not take any locale into account.

## Expiry ranges.

Key expirations are limited to 268435455 seconds (2^28 - 1, about 8.5 years) from the command's
execution time. EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, SET with EX/PX/EXAT/PXAT, SETEX, PSETEX,
GETEX with EX/PX/EXAT/PXAT, and RESTORE (including ABSTTL) clamp deadlines beyond this limit.
GETEX returns the existing string value when its expiration is clamped. Clamping does not replace
command-specific validation of invalid or overflowing expiry values; GETEX reports invalid-expiry
errors instead of returning null for an existing string key. Memcached absolute exptimes beyond the
limit are clamped the same way, including GAT and GATS.

Hash field expirations reject TTLs beyond the same 268435455-second limit instead of clamping them.

### EXPIRE flags
EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT accept NX together with GT or LT: the expiry is set when
the key has none, otherwise GT or LT alone decides. Redis rejects these combinations as
incompatible.

## Lua
We use lua 5.4.4 that has been released in 2022.
That means we also support [lua integers](https://github.com/redis/redis/issues/5261).

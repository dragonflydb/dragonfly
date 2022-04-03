# Differences with Redis

## String lengths, indices.

String sizes are limited to 256MB.
Indices (say in GETRANGE and SETRANGE commands) should be signed 32 bit integers in range
[-2147483647, 2147483648].

## Expiry ranges.
Expirations are limited to 365 days. For commands with millisecond precision like PEXPIRE or PSETEX,
expirations greater than 2^25ms are quietly rounded to the nearest second loosing precision of ~0.002%.


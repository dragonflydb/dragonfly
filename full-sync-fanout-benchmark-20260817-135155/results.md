## Full-sync fanout benchmark

Configuration: requested data size 1.00 GiB, one master, 1–10 replicas, 10 proactors per Dragonfly process, 10 random-command client threads, a 1-second fanout collection window, and io_uring (default; locked-memory limit: 3.83 GiB).

| Replicas | Without fanout | With fanout | Result | Random-command ops: without / with |
| ---: | ---: | ---: | --- | ---: |
| 1 | 0.37 s | 1.43 s | 289.5% slower | 8,755 / 99,927 |
| 2 | 0.68 s | 1.60 s | 135.8% slower | 5,149 / 99,520 |
| 3 | 1.05 s | 1.76 s | 68.1% slower | 7,760 / 97,721 |
| 4 | 1.40 s | 1.97 s | 40.6% slower | 6,413 / 98,675 |
| 5 | 1.82 s | 2.19 s | 20.2% slower | 6,496 / 71,730 |
| 6 | 2.21 s | 2.43 s | 10.1% slower | 5,507 / 98,388 |
| 7 | 2.46 s | 2.62 s | 6.6% slower | 5,283 / 99,209 |
| 8 | 2.87 s | 2.83 s | 1.4% faster (1.01×) | 6,620 / 97,033 |
| 9 | 3.25 s | 3.04 s | 6.5% faster (1.07×) | 5,896 / 99,458 |
| 10 | 3.53 s | 2.26 s | 36.0% faster (1.56×) | 3,367 / 10,134 |

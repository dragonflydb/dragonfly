Tool for benchmarking vector search with randomized vectors.

## Logic
The tool connects to the Redis/Dragonfly instance.
1. Checks if the database has enough data (at least 50% of requested `-n`). If not, it **flushes the DB** and generates random vectors.
2. Checks if the index `idx` exists. If not, it creates it.
3. Runs concurrent search queries and reports latency/QPS.

## Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `-n` | 50000 | **Number of vectors** to populate if the DB is empty. |
| `-q` | 1000 | **Total number of queries** to run during the benchmark. |
| `-t` | 8 | **Query threads**. Number of concurrent workers sending queries. |
| `-d` | 100 | **Vector dimension**. Size of the float32 vectors. |
| `-k` | 10 | **Top K**. Number of nearest neighbors to retrieve per query. |
| `-p` | 6379 | **Port** of the server. |
| `-h` | localhost | **Host** of the server. |

Run with `-h` (help) to see these defaults in the tool itself.

#!/usr/bin/env python3
import redis
import time


def main():
    max_unaccounted = 200 * 1024 * 1024  # 200mb

    client = redis.Redis(decode_responses=True)
    info = client.info("server")
    # Check version upgrade finsihed from last released version to last weekly docker build
    assert info["dragonfly_version"] == "df-HEAD-HASH-NOTFOUND"

    info = client.info("memory")
    print(f'Used memory {info["used_memory"]}, rss {info["used_memory_rss"]}')
    assert info["used_memory_rss"] - info["used_memory"] < max_unaccounted

    info = client.info("replication")
    assert info["role"] == "master"
    replication_state = info["slave0"]
    assert replication_state["state"] == "online"

    def is_zero_lag(replication_state):
        return replication_state["lag"] == 0

    # Wait for 10 seconds for lag to be zero
    for _ in range(10):
        if is_zero_lag(replication_state):
            break
        time.sleep(1)
        replication_state = client.info("replication")["slave0"]

    if replication_state["lag"] != 0:
        print(f"Lag is bad, expected 0, got {replication_state['lag']}")
        info = client.info("all")
        print(f"Info all output: {info}")
        assert False


if __name__ == "__main__":
    main()

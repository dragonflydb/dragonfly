#!/usr/bin/env python3

import argparse
import random
import string
import redis as rclient
import uuid
import time


def fill_set(args, redis: rclient.Redis):
    for j in range(args.num):
        token = uuid.uuid1().hex
        # print(token)
        key = f"USER_OTP:{token}"
        arr = []
        for i in range(30):
            otp = "".join(random.choices(string.ascii_uppercase + string.digits, k=12))
            arr.append(otp)
        redis.execute_command("sadd", key, *arr)


def fill_hset(args, redis):
    for j in range(args.num):
        token = uuid.uuid1().hex
        key = f"USER_INFO:{token}"
        phone = f"555-999-{j}"
        user_id = "user" * 5 + f"-{j}"
        redis.hset(key, "phone", phone)
        redis.hset(key, "user_id", user_id)
        redis.hset(key, "login_time", time.time())


def main():
    parser = argparse.ArgumentParser(description="fill hset entities")
    parser.add_argument("-p", type=int, help="redis port", dest="port", default=6380)
    parser.add_argument("-n", type=int, help="number of keys", dest="num", default=10000)
    parser.add_argument(
        "--type", type=str, choices=["hset", "set"], help="set type", default="hset"
    )

    args = parser.parse_args()
    redis = rclient.Redis(host="localhost", port=args.port, db=0)
    if args.type == "hset":
        fill_hset(args, redis)
    elif args.type == "set":
        fill_set(args, redis)


if __name__ == "__main__":
    main()

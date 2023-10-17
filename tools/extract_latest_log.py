#!/usr/bin/env python3

"""Extract the most recent INFO log from a directory."""

import argparse
import os


def main():
    parser = argparse.ArgumentParser(description="Extract the most recent INFO log in a directory.")
    parser.add_argument("--path", type=str, required=True, help="Path to the logs")

    args = parser.parse_args()
    files = [f for f in os.listdir(args.path) if os.path.isfile(os.path.join(args.path, f))]
    filtered_files = [name for name in files if ".log.INFO" in name and "dragonfly" in name]
    filtered_files.sort()
    print(os.path.join(args.path, filtered_files[-1]))


if __name__ == "__main__":
    main()

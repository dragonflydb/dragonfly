#!/usr/bin/env python3
import argparse
import asyncio
import logging
import asyncssh
import yaml
import os
import types
from dataclasses import dataclass


@dataclass
class Host:
    address: str
    username: str
    key_path: str
    name: str = None


def load_hosts_from_config(config_file: str) -> list[Host]:
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)  # Use yaml.safe_load()

    default_user = config.get("default_user")
    default_key_path = config.get("default_key_path")

    hosts = config.get("hosts", [])
    if not hosts:
        raise ValueError("No hosts found in the configuration file.")

    host_list = []
    id = 0
    for host_config in hosts:
        id += 1
        address = host_config.get("address")
        if not address:
            logging.warning("Skipping host with missing 'address'")
            continue

        username = host_config.get("username", default_user)
        key_path = host_config.get("key_path", default_key_path)
        name = host_config.get("name", f"host{id}")
        host_list.append(Host(address, username, key_path, name))

    return host_list


async def remote_read(reader: asyncssh.SSHReader, dest_file: str):
    with open(dest_file, "wb") as f:  # Open file in binary write mode
        while True:
            try:
                stdout_data: str = await reader.read(4096)  # Read in chunks
                if not stdout_data:  # Check if stream is finished
                    break
                f.write(stdout_data.encode())  # Write to file in chunks (binary mode!)
                f.flush()  # Flush to disk to see output immediately
            except Exception as e:
                logging.exception(f"Error reading from stream: {e}")
                break


async def run_command(hname, host, command, username, key_path=None):
    """Connects to a host and runs a command."""
    try:
        client_keys = []  # Initialize client_keys list
        if key_path:
            client_keys.append(os.path.expanduser(key_path))  # Add path to client_keys
        logging.info(f"Connecting to {username}@{host} and running command: {command}")
        async with asyncssh.connect(
            host, username=username, client_keys=client_keys, known_hosts=None
        ) as conn:
            async with conn.create_process(
                command, stdout=asyncssh.PIPE, stderr=asyncssh.PIPE
            ) as process:  # Use create_process for streaming
                await remote_read(process.stdout, f"{hname}.log")

                # Handle stderr (optional):
                stderr_data = await process.stderr.read()
                if stderr_data:
                    print(f"stderr from {host}: {stderr_data}")  # Decode if needed

                await process.wait()  # Wait for the process to finish (if it ever does)
                print(f"Command on {host} finished (or interrupted).")
    except Exception as e:
        logging.exception(f"Error connecting to {host}", exc_info=e)
        return None  # Return None if there's an error


async def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Run memtier_benchmark on remote hosts.")
    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("-c", "--clients", type=int, default=50, help="Number of clients")
    parser.add_argument("-t", "--threads", type=int, default=4, help="Number of threads")
    parser.add_argument("-d", "--data-width", type=int, default=64, help="Size of the records")
    parser.add_argument("--time", type=int, default=30, help="time in seconds")
    parser.add_argument("--hosts-file", help="Path to hosts.yml config file")

    try:
        args = parser.parse_args()
        if not args.hosts_file:
            args.hosts_file = os.path.join(os.path.dirname(__file__), "hosts.yml")
        hosts = load_hosts_from_config(args.hosts_file)

        memtier_command = (
            f"memtier_benchmark -s {args.host} -p {args.port} -c {args.clients} "
            + f"-t {args.threads} -d {args.data_width} --hide-histogram --distinct-client-seed "
            + f"--test-time={args.time}"
        )
        tasks = [
            run_command(host.name, host.address, memtier_command, host.username, host.key_path)
            for host in hosts
        ]

        results = await asyncio.gather(*tasks)
        # Process results as needed
    except FileNotFoundError:
        print(f"Error: hosts.yaml file not found in {args.hosts_file}")  # Updated file name
    except yaml.YAMLError as e:  # More specific exception
        print(f"Error parsing YAML file: {e}")  # Updated message
    except ValueError as e:
        print(e)
    except Exception as e:
        logging.exception(f"A general error occurred", exc_info=e)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import socket
import sys
import time
import os
import json
import threading
import signal
import argparse
from redis_commands import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_COMMANDS,
    DATA_TYPES,
    ARG_TYPE_MAP,
    DICT_MIX_RATIO,
    DICT_FILE,
    COMMANDS_LOG_FILE,
    CRASH_LOG_FILE,
    INPUT_DIR,
    SPECIAL_CHARS,
    ESCAPED_CHARS,
    COMMAND_SOURCES,
    load_input_dict,
    enhance_data_types,
)

# Constants
AFL_PERSISTENT = os.getenv("AFL_PERSISTENT", "0") == "1"
AFL_INST_LIBS = os.getenv("AFL_INST_LIBS", "0") == "1"
MAX_COMMANDS_PER_TEST = 20  # Maximum number of commands in one test
MAX_PARALLEL_TESTS = 10  # Maximum number of parallel tests
MAX_COMMAND_LEN = 1024  # Maximum command length
TIMEOUT_SECONDS = 5  # Timeout for command execution

# Load input values
INPUT_VALUES = load_input_dict()

# Load dictionary values if exists
DICT_VALUES = []
if os.path.exists(DICT_FILE):
    try:
        with open(DICT_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and line.startswith('"') and line.endswith('"'):
                    DICT_VALUES.append(line[1:-1])
    except Exception as e:
        print(f"Error loading dictionary file: {e}")

# Initialize enhanced data types
enhance_data_types()


# Functions for working with Redis RESP protocol
def encode_resp(data):
    """Encodes Python data to RESP format"""
    if isinstance(data, str):
        return f"${len(data)}\r\n{data}\r\n"
    elif isinstance(data, int):
        return f":{data}\r\n"
    elif isinstance(data, list):
        resp = f"*{len(data)}\r\n"
        for item in data:
            resp += encode_resp(item)
        return resp
    elif data is None:
        return "$-1\r\n"
    else:
        return f"+{data}\r\n"


def decode_resp(data):
    """Decodes RESP format to Python data"""
    if not data:
        return None

    data_type = data[0]
    if data_type == b"+":  # Simple String
        return data[1:].split(b"\r\n")[0].decode("utf-8", errors="ignore")
    elif data_type == b"-":  # Error
        return {"error": data[1:].split(b"\r\n")[0].decode("utf-8", errors="ignore")}
    elif data_type == b":":  # Integer
        return int(data[1:].split(b"\r\n")[0])
    elif data_type == b"$":  # Bulk String
        length = int(data[1:].split(b"\r\n")[0])
        if length == -1:
            return None
        start = data.find(b"\r\n") + 2
        return data[start : start + length].decode("utf-8", errors="ignore")
    elif data_type == b"*":  # Array
        parts = data.split(b"\r\n")
        length = int(parts[0][1:])
        if length == -1:
            return None

        result = []
        part_data = b"\r\n".join(parts[1:])
        pos = 0
        for _ in range(length):
            if pos >= len(part_data):
                break

            if part_data[pos] == ord(b"$"):
                # Bulk String
                end_pos = part_data.find(b"\r\n", pos)
                bulk_len = int(part_data[pos + 1 : end_pos])
                if bulk_len == -1:
                    result.append(None)
                    pos = end_pos + 2
                else:
                    string_start = end_pos + 2
                    string_end = string_start + bulk_len
                    result.append(
                        part_data[string_start:string_end].decode("utf-8", errors="ignore")
                    )
                    pos = string_end + 2
            elif part_data[pos] == ord(b":"):
                # Integer
                end_pos = part_data.find(b"\r\n", pos)
                result.append(int(part_data[pos + 1 : end_pos]))
                pos = end_pos + 2
            elif part_data[pos] == ord(b"+"):
                # Simple String
                end_pos = part_data.find(b"\r\n", pos)
                result.append(part_data[pos + 1 : end_pos].decode("utf-8", errors="ignore"))
                pos = end_pos + 2
            elif part_data[pos] == ord(b"-"):
                # Error
                end_pos = part_data.find(b"\r\n", pos)
                result.append(
                    {"error": part_data[pos + 1 : end_pos].decode("utf-8", errors="ignore")}
                )
                pos = end_pos + 2
            else:
                # Unknown data type
                break

        return result
    else:
        return data.decode("utf-8", errors="ignore")


class RedisClient:
    """Class for interacting with Redis server"""

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, timeout=TIMEOUT_SECONDS):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock = None
        print(f"Initializing connection to Redis at {self.host}:{self.port}")
        self.connect()

    def connect(self):
        """Connecting to Redis server"""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))
            print(f"Successfully connected to Redis at {self.host}:{self.port}")
            return True
        except (socket.error, socket.timeout) as e:
            print(f"Connection error to Redis at {self.host}:{self.port}: {e}")
            return False

    def close(self):
        """Closing connection to Redis server"""
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None

    def execute_command(self, command, *args):
        """Executing Redis command"""
        if not self.sock and not self.connect():
            return {"error": f"No connection to Redis at {self.host}:{self.port}"}

        try:
            # Forming RESP command
            cmd_parts = [command] + list(args)
            resp_command = encode_resp(cmd_parts)

            # Sending command
            self.sock.sendall(resp_command.encode("utf-8"))

            # Receiving response
            data = b""
            while True:
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\r\n" in data:  # Checking if we received full response
                    break

            # Decoding response
            return decode_resp(data)
        except (socket.error, socket.timeout) as e:
            return {"error": f"Command execution error: {e}"}
        except Exception as e:
            return {"error": f"Unknown error: {e}"}


class RedisCommandGenerator:
    """Class for generating random Redis commands"""

    @staticmethod
    def get_value_from_dictionary(arg_type):
        """Gets a value from dictionary or input values if available"""
        # Random choice between input values and dictionary values
        if INPUT_VALUES and random.random() < 0.5:
            return random.choice(INPUT_VALUES)
        elif DICT_VALUES:
            return random.choice(DICT_VALUES)
        return None

    @staticmethod
    def generate_random_arg(arg_type):
        """Generates a random argument of a given type with mixed sources"""
        # Try to get value from dictionary based on mix ratio
        if random.random() < DICT_MIX_RATIO:
            dict_value = RedisCommandGenerator.get_value_from_dictionary(arg_type)
            if dict_value:
                return dict_value

        # If no dictionary value or ratio check failed, generate a value
        if callable(arg_type):
            return arg_type()
        elif arg_type in ARG_TYPE_MAP:
            type_name = ARG_TYPE_MAP[arg_type]
            if callable(type_name):
                return type_name()

            # Decide if we should use a special variant
            variant_decision = random.random()
            if variant_decision < 0.2 and f"special_{type_name}" in DATA_TYPES:
                return DATA_TYPES[f"special_{type_name}"]()
            elif variant_decision < 0.4 and f"escaped_{type_name}" in DATA_TYPES:
                return DATA_TYPES[f"escaped_{type_name}"]()
            elif variant_decision < 0.6 and type_name in ["string", "value", "message", "element"]:
                return DATA_TYPES["mixed_string"]()
            elif variant_decision < 0.8 and type_name in ["string", "value", "message", "element"]:
                return DATA_TYPES["binary_string"]()
            else:
                return DATA_TYPES[type_name]()

        return arg_type  # Returns as is if type not found

    @staticmethod
    def generate_random_command():
        """Generates a random Redis command with arguments"""
        command = random.choice(list(REDIS_COMMANDS.keys()))
        command_info = REDIS_COMMANDS[command]

        args = []
        for arg in command_info["args"]:
            args.append(RedisCommandGenerator.generate_random_arg(arg))

        # Adds random optional arguments
        if (
            command_info["optional_args"] and random.random() < 0.7
        ):  # Increased probability to include optional args
            for opt_arg in random.sample(
                command_info["optional_args"], random.randint(0, len(command_info["optional_args"]))
            ):
                if " " in opt_arg:  # If argument consists of multiple parts
                    opt_parts = opt_arg.split(" ")
                    if "|" in opt_parts[0]:  # If it's a choice between multiple options
                        choices = opt_parts[0].split("|")
                        args.append(random.choice(choices))
                    else:
                        args.append(opt_parts[0])
                        if len(opt_parts) > 1:
                            args.append(RedisCommandGenerator.generate_random_arg(opt_parts[1]))
                else:
                    args.append(opt_arg)

        return command, args


class TestCase:
    """Class for creating and executing test cases"""

    def __init__(self, seed=None):
        if seed is not None:
            random.seed(seed)
        self.commands = []
        self.results = []
        self.redis_client = None

    def generate_test_case(self, num_commands=None):
        """Generates a test case with random commands"""
        if num_commands is None:
            num_commands = random.randint(1, MAX_COMMANDS_PER_TEST)

        generated_commands = []
        for _ in range(num_commands):
            command, args = RedisCommandGenerator.generate_random_command()
            generated_commands.append((command, args))

        self.commands = generated_commands
        return self.commands

    def execute_test_case(self):
        """Executes a test case on Redis server"""
        self.redis_client = RedisClient()
        self.results = []

        for command, args in self.commands:
            try:
                result = self.redis_client.execute_command(command, *args)
                self.results.append({"command": command, "args": args, "result": result})
            except Exception as e:
                self.results.append({"command": command, "args": args, "error": str(e)})

        self.redis_client.close()
        return self.results

    def save_to_file(self, filename):
        """Saves a test case to file"""
        with open(filename, "w", encoding="utf-8") as f:
            data = {"commands": self.commands, "results": self.results}
            json.dump(data, f, ensure_ascii=False, indent=2)

    @staticmethod
    def load_from_file(filename):
        """Loads a test case from file"""
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)

        test_case = TestCase()
        test_case.commands = data["commands"]
        if "results" in data:
            test_case.results = data["results"]

        return test_case


class AFLFuzzer:
    """Class for integrating with AFL++"""

    def __init__(self):
        self.afl_input = None
        self.test_cases = []
        self.results = []
        self.stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "error_executions": 0,
            "timeouts": 0,
            "crashes": 0,
        }

        print(f"AFLFuzzer initialized with target: {REDIS_HOST}:{REDIS_PORT}")

        # Setting up signals for interaction with AFL++
        if AFL_PERSISTENT:
            signal.signal(signal.SIGTERM, self.handle_sigterm)
            signal.signal(signal.SIGINT, self.handle_sigint)

    def handle_sigterm(self, signum, frame):
        """Handling SIGTERM signal from AFL++"""
        sys.exit(0)

    def handle_sigint(self, signum, frame):
        """Handling SIGINT signal from AFL++"""
        sys.exit(0)

    def read_afl_input(self):
        """Reads input data from AFL++"""
        try:
            self.afl_input = sys.stdin.buffer.read()
            print(f"Read {len(self.afl_input)} bytes from AFL++ input")
            return True
        except Exception as e:
            print(f"Error reading input data from AFL++: {e}")
            return False

    def parse_afl_input(self):
        """Parses input data from AFL++ into Redis commands"""
        if not self.afl_input:
            print("No input data available")
            return False

        try:
            # Splitting input data into lines
            lines = self.afl_input.split(b"\n")
            parsed_commands = []

            for line in lines:
                if not line:
                    continue

                # Parsing command and arguments
                parts = line.decode("utf-8", errors="ignore").strip().split(" ")
                if not parts:
                    continue

                command = parts[0].upper()
                args = parts[1:] if len(parts) > 1 else []

                # Checking if there's such a command
                if command in REDIS_COMMANDS:
                    parsed_commands.append((command, args))

            print(f"Parsed {len(parsed_commands)} commands from input")

            # Always generate a mix of parsed and random commands
            test_case_seed = (
                int.from_bytes(self.afl_input[:4], byteorder="little")
                if len(self.afl_input) >= 4
                else None
            )
            test_case = TestCase(seed=test_case_seed)

            # Generate random number of commands
            num_commands = random.randint(1, MAX_COMMANDS_PER_TEST)

            # Mix parsed commands with random ones
            if parsed_commands:
                # Use at least half of parsed commands if available
                num_parsed = min(len(parsed_commands), num_commands // 2 + 1)
                self.test_cases = random.sample(parsed_commands, num_parsed)

                # Add random commands to reach the desired number
                while len(self.test_cases) < num_commands:
                    command, args = RedisCommandGenerator.generate_random_command()
                    self.test_cases.append((command, args))
            else:
                # If no parsed commands, generate all random ones
                self.test_cases = test_case.generate_test_case(num_commands)

            # Shuffle the commands
            random.shuffle(self.test_cases)

            print(f"Generated {len(self.test_cases)} total commands for testing")
            return True
        except Exception as e:
            print(f"Error parsing input data from AFL++: {e}")
            # In case of error, generate random commands
            test_case = TestCase()
            self.test_cases = test_case.generate_test_case()
            print(f"Falling back to {len(self.test_cases)} random commands")
            return True

    def format_command_for_cli(self, command, args):
        """Format command for redis-cli input"""
        formatted_args = []
        for arg in args:
            arg_str = str(arg)

            # Check for binary data and special characters
            try:
                # Try to decode as UTF-8 if it's bytes
                if isinstance(arg, bytes):
                    arg_str = arg.decode("utf-8", errors="backslashreplace")
            except:
                # If there's a decoding error, use character representation with escaping
                arg_str = repr(arg)[1:-1]  # Remove quotes from repr

            # Always use quotes for safety
            escaped_arg = arg_str.replace('"', '\\"')
            formatted_args.append('"' + escaped_arg + '"')

        return f"{command} {' '.join(formatted_args)}"

    def execute_tests(self, skip_logging=False):
        """Executes tests on Redis server"""
        print(f"Attempting to connect to Redis server at {REDIS_HOST}:{REDIS_PORT}")
        redis_client = RedisClient()

        # Verify connection works before proceeding
        if not redis_client.sock:
            print(f"ERROR: Failed to connect to Redis server at {REDIS_HOST}:{REDIS_PORT}")
            # Try a simple PING to confirm it's actually unreachable
            try:
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_sock.settimeout(5)
                test_sock.connect((REDIS_HOST, REDIS_PORT))
                test_sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                response = test_sock.recv(1024)
                print(f"Manual PING test response: {response}")
                test_sock.close()
            except Exception as e:
                print(f"Manual connection test failed: {e}")

            # Give up on executing tests
            self.stats["error_executions"] += len(self.test_cases)
            return []

        print(
            f"Executing {len(self.test_cases)} commands on Redis server at {REDIS_HOST}:{REDIS_PORT}"
        )

        # Open commands log file in append mode if not skipping logging
        if not skip_logging:
            with open(COMMANDS_LOG_FILE, "a") as log_file:
                for command, args in self.test_cases:
                    # Log command to file in redis-cli compatible format
                    cli_formatted_command = self.format_command_for_cli(command, args)
                    log_file.write(f"{cli_formatted_command}\n")
                    log_file.flush()  # Ensure command is written immediately
                    print(f"Logged command: {cli_formatted_command}")

        # Execute all commands
        current_test_commands = []
        crash_detected = False

        for idx, (command, args) in enumerate(self.test_cases):
            # Add current command to test sequence
            current_test_commands.append((command, args))

            print(f"Executing command {idx+1}/{len(self.test_cases)}: {command} {args}")
            try:
                # Execute command
                result = redis_client.execute_command(command, *args)
                self.results.append({"command": command, "args": args, "result": result})
                self.stats["successful_executions"] += 1
                print(f"Command succeeded: {command}")
            except socket.timeout:
                self.results.append({"command": command, "args": args, "error": "Timeout"})
                self.stats["timeouts"] += 1
                print(f"Command timeout: {command}")
            except Exception as e:
                self.results.append({"command": command, "args": args, "error": str(e)})
                self.stats["error_executions"] += 1
                print(f"Command error: {command} - {str(e)}")

                # Check for possible crash
                error_msg = str(e).lower()
                if (
                    "connection" in error_msg
                    or "broken pipe" in error_msg
                    or "reset by peer" in error_msg
                ):
                    crash_detected = True
                    self.stats["crashes"] += 1
                    print(f"Crash detected with command: {command}")
                    # Record the command sequence that caused the crash
                    with open(CRASH_LOG_FILE, "a") as crash_file:
                        crash_file.write(f"# --- CRASH SEQUENCE START ---\n")
                        for cmd, arg in current_test_commands:
                            cmd_str = self.format_command_for_cli(cmd, arg)
                            crash_file.write(f"{cmd_str}\n")
                        crash_file.write(f"# --- CRASH SEQUENCE END ---\n\n")
                    break

            self.stats["total_executions"] += 1

        redis_client.close()
        print(
            f"Test execution complete. Total: {self.stats['total_executions']}, Successful: {self.stats['successful_executions']}, Errors: {self.stats['error_executions']}"
        )
        return self.results

    def run(self):
        """Main method for running fuzzing"""
        if AFL_PERSISTENT:
            while True:
                # Clearing state
                self.afl_input = None
                self.test_cases = []
                self.results = []

                # Reading and processing input data from AFL++
                if not self.read_afl_input() or not self.parse_afl_input():
                    sys.exit(1)

                # Executing tests
                self.execute_tests()

                # Analyzing results
                self.analyze_results()

                # Notifying AFL++ about iteration completion
                print("DONE")
                sys.stdout.flush()
        else:
            # For running outside AFL++
            if not self.read_afl_input() or not self.parse_afl_input():
                # If no input data, generate random commands
                test_case = TestCase()
                self.test_cases = test_case.generate_test_case()

            # Executing tests
            self.execute_tests()

            # Analyzing results
            self.analyze_results()

            # Displaying results
            self.print_results()

    def analyze_results(self):
        """Analyzes test results"""
        # Verification is already performed in execute_tests, this method remains for compatibility
        pass

    def print_results(self):
        """Displays test results"""
        print(f"Executed tests: {self.stats['total_executions']}")
        print(f"Successful executions: {self.stats['successful_executions']}")
        print(f"Errors: {self.stats['error_executions']}")
        print(f"Timeouts: {self.stats['timeouts']}")
        print(f"Crashes: {self.stats['crashes']}")

        print("\nDetailed results:")
        for idx, result in enumerate(self.results):
            print(f"{idx+1}. Command: {result['command']} {' '.join(result['args'])}")
            if "error" in result:
                print(f"   Error: {result['error']}")
            else:
                print(f"   Result: {result['result']}")
            print()


def parse_args():
    """Parsing command line arguments"""
    parser = argparse.ArgumentParser(description="Redis fuzzer for testing Dragonfly using AFL++")
    parser.add_argument("--host", default=REDIS_HOST, help=f"Redis host (default: {REDIS_HOST})")
    parser.add_argument(
        "--port", type=int, default=REDIS_PORT, help=f"Redis port (default: {REDIS_PORT})"
    )
    parser.add_argument(
        "--commands",
        type=int,
        default=MAX_COMMANDS_PER_TEST,
        help=f"Number of commands in test (default: {MAX_COMMANDS_PER_TEST})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Setting global variables from arguments
    global REDIS_HOST, REDIS_PORT, MAX_COMMANDS_PER_TEST
    REDIS_HOST = args.host
    REDIS_PORT = args.port
    MAX_COMMANDS_PER_TEST = args.commands

    print(f"Starting fuzzer with target: {REDIS_HOST}:{REDIS_PORT}")

    # Quick connection test
    print(f"Testing connection to Redis server at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_sock.settimeout(5)
        test_sock.connect((REDIS_HOST, REDIS_PORT))
        test_sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        response = test_sock.recv(1024)
        print(f"Connection test successful! Response: {response}")
        test_sock.close()
    except Exception as e:
        print(f"WARNING: Connection test failed: {e}")
        print(f"Please verify that Redis server is running at {REDIS_HOST}:{REDIS_PORT}")
        if input("Continue anyway? (y/n): ").lower() != "y":
            print("Exiting...")
            sys.exit(1)

    # Always reload dictionary values for each run
    global DICT_VALUES, INPUT_VALUES
    DICT_VALUES = []
    if os.path.exists(DICT_FILE):
        try:
            with open(DICT_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and line.startswith('"') and line.endswith('"'):
                        DICT_VALUES.append(line[1:-1])
        except Exception as e:
            print(f"Error loading dictionary file: {e}")

    # Always reload input values
    INPUT_VALUES = load_input_dict()

    # Create directory for log files if needed
    for log_file in [COMMANDS_LOG_FILE, CRASH_LOG_FILE]:
        log_dir = os.path.dirname(log_file)
        os.makedirs(log_dir, exist_ok=True)

    # Running fuzzing with mixed strategy always enabled
    fuzzer = AFLFuzzer()
    fuzzer.run()


if __name__ == "__main__":
    main()

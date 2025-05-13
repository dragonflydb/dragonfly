#!/bin/bash

# Default constants
DEFAULT_REDIS_HOST="127.0.0.1"
DEFAULT_REDIS_PORT=6379
DEFAULT_COMMANDS_LOG="$(dirname "$(readlink -f "$0")")/commands.log"

# Variables that can be overridden through arguments
REDIS_HOST=$DEFAULT_REDIS_HOST
REDIS_PORT=$DEFAULT_REDIS_PORT
COMMANDS_FILE=$DEFAULT_COMMANDS_LOG

# Interrupt handling
trap 'echo -e "\nExecution interrupted by user."; exit 1' INT

# Execute Redis command through redis-cli
execute_redis_command() {
    local command=$1
    local host=$2
    local port=$3
    local first_word=$(echo "$command" | awk '{print $1}')
    local full_command=$(echo "$command" | tr '[:lower:]' '[:upper:]')

    # Check if this is a blocking subscription command
    if [[ "$first_word" == "SUBSCRIBE" || "$first_word" == "PSUBSCRIBE" || "$first_word" == "SSUBSCRIBE" ||
          "$first_word" == "MONITOR" || "$first_word" == "BLPOP" || "$first_word" == "BRPOP" ||
          "$first_word" == "BLMOVE" || "$first_word" == "BZPOPMIN" || "$first_word" == "BZPOPMAX" ||
          ("$first_word" == "XREAD" && "$full_command" =~ "BLOCK") ]]; then

        echo "Detected blocking command. Setting 5 second timeout..."
        timeout 5 redis-cli -h "$host" -p "$port" <<< "$command"
        local exit_code=$?

        if [ $exit_code -eq 124 ]; then # 124 is the timeout exit code
            echo "Command timed out after 5 seconds, which is expected for blocking commands."
            return 0 # Consider this a success
        elif [ $exit_code -ne 0 ]; then
            echo "Command failed with exit code $exit_code"
            return $exit_code
        fi
        return 0
    else
        # For non-blocking commands, execute normally
        output=$(echo "$command" | redis-cli -h "$host" -p "$port" 2>&1)
        local exit_code=$?

        # Check for connection errors
        if [[ "$output" == *"Could not connect"* || "$output" == *"Connection refused"* ||
              "$output" == *"Connection timed out"* || "$output" == *"Connection error"* ]]; then
            echo "Error: Connection to Redis failed"
            echo "Output: $output"
            return 100 # Special code for connection errors
        fi

        echo "$output"
        return $exit_code
    fi
}

# Display help
print_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Tool for sending Redis commands to find crashes"
    echo
    echo "Options:"
    echo "  --host HOST           Redis host (default: $DEFAULT_REDIS_HOST)"
    echo "  --port PORT           Redis port (default: $DEFAULT_REDIS_PORT)"
    echo "  --commands FILE       Commands file (default: $DEFAULT_COMMANDS_LOG)"
    echo "  -h, --help            Show this help message"
    echo
}

# Process arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            REDIS_HOST="$2"
            shift 2
            ;;
        --port)
            REDIS_PORT="$2"
            shift 2
            ;;
        --commands)
            COMMANDS_FILE="$2"
            shift 2
            ;;
        -h|--help)
            print_help
            exit 0
            ;;
        *)
            echo "Unknown parameter: $1"
            print_help
            exit 1
            ;;
    esac
done

# Check for redis-cli availability
if ! command -v redis-cli &> /dev/null; then
    echo "Error: redis-cli not found. Make sure redis-cli is installed and available in PATH."
    exit 1
fi

# Check if commands file exists
if [ ! -f "$COMMANDS_FILE" ]; then
    echo "Error: Commands file not found: $COMMANDS_FILE"
    exit 1
fi

echo "Starting Redis Crash Finder:"
echo "Host: $REDIS_HOST"
echo "Port: $REDIS_PORT"
echo "Commands file: $COMMANDS_FILE"

# Process commands file
echo "Reading commands from file: $COMMANDS_FILE"
echo "Connecting to Redis at $REDIS_HOST:$REDIS_PORT"

LINE_NUMBER=0
SUCCESS=true

while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" == \#* ]]; then
        continue
    fi

    ((LINE_NUMBER++))

    # Execute command
    echo "--------------------------------------------------"
    echo "Executing command [$LINE_NUMBER]: $line"

    execute_redis_command "$line" "$REDIS_HOST" "$REDIS_PORT"
    exit_code=$?

    # Check for connection error
    if [ $exit_code -eq 100 ]; then
        echo "Connection to Redis lost. Stopping execution."
        SUCCESS=false
        break
    elif [ $exit_code -ne 0 ]; then
        echo "Command failed with exit code $exit_code"
    fi

    # Small pause between commands for stability
    sleep 0.1

done < "$COMMANDS_FILE"

# Display result
if [ "$SUCCESS" = true ]; then
    echo -e "\nAll commands executed successfully!"
    exit 0
else
    echo -e "\nCommand execution interrupted due to a connection error."
    exit 1
fi

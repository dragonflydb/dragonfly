#!/bin/bash
# Hook to automatically format files after Edit/Write operations
# Filters out src/redis directory from formatting

# Read JSON input from stdin
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Skip if no file path
if [ -z "$FILE_PATH" ]; then
  exit 0
fi

# Skip if file is in src/redis directory
if [[ "$FILE_PATH" == */src/redis/* ]]; then
  echo "Skipping formatting for src/redis file: $FILE_PATH" >&2
  exit 0
fi

# Skip if file doesn't exist
if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

# Run pre-commit on the file
pre-commit run --files "$FILE_PATH" 2>&1

# Always exit 0 to not block the operation even if formatting fails
exit 0

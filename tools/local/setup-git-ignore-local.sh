#!/bin/bash

# Exit on error
set -e

# Path to the file list
FILES_LIST="$(dirname "$0")/ignore-files-list.txt"

# Function to display help information
show_help() {
    echo "Usage: ./setup-git-ignore-local.sh [OPTION]"
    echo ""
    echo "Configure Git to ignore or track local changes in specific files."
    echo ""
    echo "Options:"
    echo "  --help       Display this help message and exit"
    echo "  --unignore   Configure Git to track changes in previously ignored files"
    echo "  --reset      Same as --unignore"
    echo ""
    echo "Without options, the script will configure Git to ignore local changes"
    echo "in files listed in $(basename "$FILES_LIST")"
    echo ""
    echo "Details:"
    echo "  When running without parameters (default 'ignore' mode):"
    echo "  - Configured files won't show up in git status when locally modified"
    echo "  - This is useful for local configuration files you don't want to commit"
    echo ""
    echo "  When running with --unignore or --reset:"
    echo "  - Configured files will show up in git status when locally modified"
    echo "  - This is needed if you actually want to commit changes to these files"
    echo ""
    echo "  Workflow for committing changes to an ignored file:"
    echo "  1. Run './setup-git-ignore-local.sh --unignore' (or for a specific file:"
    echo "     'git update-index --no-assume-unchanged <file_path>')"
    echo "  2. Make and commit your changes"
    echo "  3. Run './setup-git-ignore-local.sh' again to re-ignore the files"
    echo "     (or for a specific file: 'git update-index --assume-unchanged <file_path>')"
    echo ""
    echo "Examples:"
    echo "  ./setup-git-ignore-local.sh            # Ignore local changes"
    echo "  ./setup-git-ignore-local.sh --unignore # Track local changes"
    echo ""
    echo "File List:"
    echo "  The list of files to process is stored in $(basename "$FILES_LIST")"
    echo "  - Each line should contain a path relative to the repository root"
    echo "  - Empty lines and lines starting with # are ignored"
    echo ""
    echo "For more information, see CONTRIBUTING.md"
    exit 0
}

# Check if help was requested
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
fi

# Check if the files list exists
if [ ! -f "$FILES_LIST" ]; then
    echo "Error: Files list not found at: $FILES_LIST"
    echo "Create this file with a list of files to be processed, one per line."
    exit 1
fi

# Default mode is to ignore changes (assume-unchanged)
MODE="ignore"

# Check if a mode parameter was provided
if [ "$1" = "--unignore" ] || [ "$1" = "--reset" ]; then
    MODE="unignore"
fi

# Set the appropriate Git command based on mode
if [ "$MODE" = "ignore" ]; then
    GIT_COMMAND="--assume-unchanged"
    ACTION_PARTICIPLE="ignored"
    echo "Setting up Git to ignore local changes in specific files..."
else
    GIT_COMMAND="--no-assume-unchanged"
    ACTION_PARTICIPLE="tracked"
    echo "Setting up Git to track changes in previously ignored files..."
fi

# Process each file from the list
PROCESSED_COUNT=0
ERRORS_COUNT=0

while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" == \#* ]]; then
        continue
    fi

    # Check if the file exists
    if [ ! -f "$line" ]; then
        echo "Warning: File '$line' does not exist, skipping..."
        continue
    fi

    # Apply the git command
    if git update-index $GIT_COMMAND "$line"; then
        echo "Successfully configured $line to be $ACTION_PARTICIPLE"
        ((PROCESSED_COUNT++))
    else
        echo "Error: Failed to configure $line"
        ((ERRORS_COUNT++))
    fi
done < "$FILES_LIST"

echo ""
echo "Summary:"
echo "- $PROCESSED_COUNT files processed successfully"
if [ "$ERRORS_COUNT" -gt 0 ]; then
    echo "- $ERRORS_COUNT errors encountered"
    echo "Git configuration completed with errors!"
else
    echo "Git configuration successfully completed!"
fi
echo ""

# Simplified output after execution
if [ "$MODE" = "ignore" ]; then
    echo "Configured files won't show up in git status when locally modified."
    echo "To commit changes to these files later, run: ./setup-git-ignore-local.sh --unignore"
else
    echo "Configured files will now show up in git status when locally modified."
    echo "After committing, run: ./setup-git-ignore-local.sh to ignore them again."
fi

echo ""
echo "For more details, run: ./setup-git-ignore-local.sh --help"

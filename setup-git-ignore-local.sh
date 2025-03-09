#!/bin/bash

# Exit on error
set -e

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
    echo "in specific files (.vscode/launch.json, .vscode/settings.json, etc.)"
    echo ""
    echo "Examples:"
    echo "  ./setup-git-ignore-local.sh           # Ignore local changes"
    echo "  ./setup-git-ignore-local.sh --unignore # Track local changes"
    echo ""
    echo "For more information, see CONTRIBUTING.md"
    exit 0
}

# Check if help was requested
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
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
    ACTION_VERB="ignore"
    ACTION_PARTICIPLE="ignored"
    echo "Setting up Git to ignore local changes in specific files..."
else
    GIT_COMMAND="--no-assume-unchanged"
    ACTION_VERB="track"
    ACTION_PARTICIPLE="tracked"
    echo "Setting up Git to track changes in previously ignored files..."
fi

# Process launch.json file
if git update-index $GIT_COMMAND .vscode/launch.json; then
    echo "Successfully configured .vscode/launch.json to be $ACTION_PARTICIPLE"
else
    echo "Error: Failed to configure .vscode/launch.json"
    exit 1
fi

# Find and configure all .db files in .vscode directory if they exist
DB_FILES=$(find .vscode -name "*.db" 2>/dev/null || true)
if [ -n "$DB_FILES" ]; then
    for file in $DB_FILES; do
        if git update-index $GIT_COMMAND "$file"; then
            echo "Successfully configured $file to be $ACTION_PARTICIPLE"
        else
            echo "Error: Failed to configure $file"
            exit 1
        fi
    done
fi

# Settings.json file if it exists
if [ -f ".vscode/settings.json" ]; then
    if git update-index $GIT_COMMAND .vscode/settings.json; then
        echo "Successfully configured .vscode/settings.json to be $ACTION_PARTICIPLE"
    else
        echo "Error: Failed to configure .vscode/settings.json"
        exit 1
    fi
fi

echo "Git configuration successfully completed!"
echo ""

if [ "$MODE" = "ignore" ]; then
    echo "Now configured files won't show up in git status when locally modified."
    echo ""
    echo "If you want to commit changes to a specific file, use:"
    echo "  ./setup-git-ignore-local.sh --unignore"
    echo "or for a specific file only:"
    echo "  git update-index --no-assume-unchanged <file_path>"
    echo ""
    echo "After committing, don't forget to run again:"
    echo "  ./setup-git-ignore-local.sh"
    echo "or for a specific file only:"
    echo "  git update-index --assume-unchanged <file_path>"
else
    echo "Now configured files will show up in git status when locally modified."
    echo ""
    echo "After committing your changes, you can run the script without parameters"
    echo "to ignore local changes again:"
    echo "  ./setup-git-ignore-local.sh"
fi

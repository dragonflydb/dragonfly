#!/bin/bash

show_help() {
    echo "Usage: $0 [PATH]"
    echo "  PATH: Optional. Relative or absolute path to start formatting from. Default is current directory."
    echo "  -h, --help: Show this help message."
}

# Parse arguments
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

START_PATH="${1:-.}"

# Function to format files in the given directory
format_repo() {
    local repo_path="$1"
    (
        cd "$repo_path" || exit 1
        git diff --name-only --cached --diff-filter=ACMRT | grep -E '\.(cc|h)$'
        git diff --name-only --diff-filter=ACMRT | grep -E '\.(cc|h)$'
        git ls-files --others --exclude-standard | grep -E '\.(cc|h)$'
    ) | sort | uniq | while read -r f; do
        if [ -f "$repo_path/$f" ]; then
            clang-format -i "$repo_path/$f"
            echo "Formatted: $repo_path/$f"
        fi
    done
}

# Format main repo
format_repo "$START_PATH"

# Format all submodules recursively
(
    cd "$START_PATH" || exit 1
    git submodule foreach --recursive '
        (
            git diff --name-only --cached --diff-filter=ACMRT | grep -E "\.(cc|h)$"
            git diff --name-only --diff-filter=ACMRT | grep -E "\.(cc|h)$"
            git ls-files --others --exclude-standard | grep -E "\.(cc|h)$"
        ) | sort | uniq | while read -r f; do
            if [ -f "$path/$f" ]; then
                clang-format -i "$path/$f"
                echo "Formatted: $path/$f"
            fi
        done
    '
)

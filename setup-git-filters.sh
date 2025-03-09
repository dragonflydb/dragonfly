#!/bin/bash

# Exit on error
set -e

# Setting up Git filter for ignoring local changes in certain files
git config filter.ignorelocalchanges.smudge cat
git config filter.ignorelocalchanges.clean cat
git config filter.ignorelocalchanges.required true

# Setting assume-unchanged flag for files
echo "Setting up Git filters for specific files..."

# Launch.json file
if git update-index --assume-unchanged .vscode/launch.json; then
    echo "Successfully configured .vscode/launch.json"
else
    echo "Error: Failed to configure .vscode/launch.json"
    exit 1
fi

# Find and configure all .db files in .vscode directory if they exist
DB_FILES=$(find .vscode -name "*.db" 2>/dev/null || true)
if [ -n "$DB_FILES" ]; then
    for file in $DB_FILES; do
        if git update-index --assume-unchanged "$file"; then
            echo "Successfully configured $file"
        else
            echo "Error: Failed to configure $file"
            exit 1
        fi
    done
fi

# Settings.json file if it exists
if [ -f ".vscode/settings.json" ]; then
    if git update-index --assume-unchanged .vscode/settings.json; then
        echo "Successfully configured .vscode/settings.json"
    else
        echo "Error: Failed to configure .vscode/settings.json"
        exit 1
    fi
fi

echo "Git filters successfully configured!"
echo ""
echo "Now configured files won't show up in git status when locally modified."
echo ""
echo "If you want to commit changes to a specific file, use:"
echo "  git update-index --no-assume-unchanged <file_path>"
echo ""
echo "After committing, don't forget to run again:"
echo "  git update-index --assume-unchanged <file_path>"

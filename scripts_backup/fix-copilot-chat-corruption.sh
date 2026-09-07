#!/usr/bin/env bash
# fix-copilot-chat-corruption.sh
#
# Safely repairs a corrupted GitHub Copilot Chat "getting the chat ready"
# hang in VS Code, WITHOUT touching your per-workspace chat history.
#
# What it does:
#   1. Backs up ~/.config/Code/User/workspaceStorage (all workspace chats)
#      to ~/vscode_chats_backup_<date> before touching anything.
#   2. Deletes only ~/.config/Code/User/globalStorage/github.copilot-chat
#      (extension runtime/embedding cache — not your chat text).
#
# Safety:
#   - Read-only until you explicitly approve with 'y'.
#   - Never deletes workspaceStorage (where your actual chats live).
#   - Aborts on any 'n' or anything other than 'y'.

set -euo pipefail

CONFIG_DIR="$HOME/.config/Code/User"
WORKSPACE_STORAGE="$CONFIG_DIR/workspaceStorage"
GLOBAL_COPILOT_CHAT="$CONFIG_DIR/globalStorage/github.copilot-chat"
BACKUP_DIR="$HOME/vscode_chats_backup_$(date +%F_%H%M%S)"

echo "=== Copilot Chat corruption fix ==="
echo

if [ ! -d "$WORKSPACE_STORAGE" ]; then
    echo "ERROR: $WORKSPACE_STORAGE not found. Nothing to back up. Aborting."
    exit 1
fi

if [ ! -d "$GLOBAL_COPILOT_CHAT" ]; then
    echo "NOTE: $GLOBAL_COPILOT_CHAT not found. It may already be clean."
    echo "Nothing to delete. Exiting."
    exit 0
fi

echo "This script will:"
echo "  1. Copy: $WORKSPACE_STORAGE"
echo "     to:   $BACKUP_DIR"
echo "  2. Delete: $GLOBAL_COPILOT_CHAT"
echo
echo "Your per-workspace chat history (in workspaceStorage) will NOT be deleted."
echo

read -r -p "Proceed? Only 'y' will continue, anything else aborts: " CONFIRM
if [ "$CONFIRM" != "y" ]; then
    echo "Aborted. No changes made."
    exit 1
fi

echo
echo "[1/2] Backing up workspaceStorage to $BACKUP_DIR ..."
cp -r "$WORKSPACE_STORAGE" "$BACKUP_DIR"
echo "Backup complete: $BACKUP_DIR"

echo
read -r -p "Backup done. Confirm deletion of corrupted global state? Only 'y' will continue: " CONFIRM2
if [ "$CONFIRM2" != "y" ]; then
    echo "Aborted before deletion. Your backup at $BACKUP_DIR is kept."
    exit 1
fi

echo "[2/2] Removing $GLOBAL_COPILOT_CHAT ..."
rm -rf "$GLOBAL_COPILOT_CHAT"
echo "Done."

echo
echo "Next steps:"
echo "  1. Fully close VS Code (all windows / remote sessions)."
echo "  2. Reopen VS Code and let the Copilot Chat extension reinitialize."
echo "  3. Your workspace chat histories will load normally from workspaceStorage."

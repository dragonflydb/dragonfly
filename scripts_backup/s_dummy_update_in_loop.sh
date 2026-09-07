#!/bin/bash

# --- Configuration ---
DS_ID="dst_ygr97xvc0"
ENV="prod"
VERSION="v1.36.9+internal"
ITERATIONS=100

# Exit immediately if a command exits with a non-zero status
# pipefail ensures that if any part of a pipe fails (like the dfadmin command), the script exits
set -e
set -o pipefail

echo "Starting stress test for datastore: $DS_ID"
echo "Target: $ITERATIONS iterations of redeployment."
echo "------------------------------------------------"

for ((i=1; i<=ITERATIONS; i++)); do
    echo "[$(date +'%H:%M:%S')] Iteration $i/$ITERATIONS"

    # --- Step 1: Polling Loop (Wait for Active) ---
    while true; do
        # Fetch status using dfadmin
        # We use grep and awk to parse the 'status: value' line from the output
        CURRENT_STATUS=$(dfadmin --env $ENV datastore show $DS_ID | grep "status:" | awk '{print $2}')

        # Clean up any potential whitespace
        CURRENT_STATUS=$(echo "$CURRENT_STATUS" | xargs)\

        if [[ "$CURRENT_STATUS" == "active" ]]; then
            echo "  -> Status is ACTIVE. Proceeding to update."
            break
        elif [[ "$CURRENT_STATUS" == "updating" ]]; then
            echo "  -> Status is UPDATING. Waiting 1s..."
            sleep 1
        else
            echo "  -> Unexpected status: '$CURRENT_STATUS'. Exiting for safety."
            exit 1
        fi
    done

    # --- Step 2: Trigger Update ---
    echo "  -> Triggering redeploy/update..."

    # pipe "yes" into the command to auto-confirm the prompt
    # --redeploy true forces the restart/update even if config hasn't changed
    echo "yes" | dfadmin --env $ENV datastore update $DS_ID \
        --dragonfly.version $VERSION \
        --redeploy true

    echo "  -> Update command sent successfully."
    echo "------------------------------------------------"

    # Optional: Short sleep to ensure the backend registers the state change before next loop
    sleep 1
done

echo "Stress test completed successfully."

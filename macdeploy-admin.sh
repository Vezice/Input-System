#!/bin/bash

# This script deploys ONLY the Admin Sheet project.
# Use this when you only changed Admin Sheet files and don't need to deploy all workers/centrals.

set -e

# Stop the script if no commit message was provided.
if [ -z "$1" ]; then
    echo -e "\033[0;31m❌ Error: Please provide a commit message.\033[0m"
    echo "Example: ./macdeploy-admin.sh 'Update admin sheet'"
    exit 1
fi

MESSAGE="$1"

echo "--- Starting Git process ---"
echo -e "\033[0;36mStep 1/3: Staging all files...\033[0m"
git add .

echo -e "\033[0;36mStep 2/3: Committing with message: \"$MESSAGE\"\033[0m"
git commit -m "$MESSAGE"

echo -e "\033[0;36mStep 3/3: Pushing to GitHub...\033[0m"
git push --force

# --- Deploy Admin Sheet only ---
echo -e "\n\033[0;32mDeploying Admin Sheet...\033[0m"

PROJECT_DIR="./Admin Sheet"
echo -e "\n\033[0;36mProcessing project in: $PROJECT_DIR\033[0m"
(
    cd "$PROJECT_DIR"
    echo "--> Pushing latest code..."
    clasp push --force

    DEPLOYMENT_ID=$(jq -r '.deploymentId' deployment.json)
    if [ -n "$DEPLOYMENT_ID" ] && [ "$DEPLOYMENT_ID" != "null" ]; then
        echo "--> Deploying project to ID: $DEPLOYMENT_ID"
        clasp deploy --deploymentId "$DEPLOYMENT_ID" --description "$MESSAGE"
    else
        echo -e "--> \033[0;33mWARNING: No deploymentId found. Skipping deployment.\033[0m"
    fi
)

echo -e "\n\033[0;32m✅ Admin Sheet deployed.\033[0m"

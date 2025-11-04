#!/bin/bash

# This script will exit immediately if any command fails.
set -e

# --- Part 1: Git Commit and Push ---

# Stop the script if no commit message was provided.
if [ -z "$1" ]; then
    echo -e "\033[0;31m❌ Error: Please provide a commit message.\033[0m"
    echo "Example: ./macdeploy.sh 'Final updates'"
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

# --- Part 2: Clasp Push and Deploy ---
echo -e "\n\033[0;32mStarting batch push and deploy for all projects...\033[0m"

while IFS= read -r file; do
    PROJECT_DIR=$(dirname "$file")
    echo -e "\n\033[0;36mProcessing project in: $PROJECT_DIR\033[0m"
    (
        cd "$PROJECT_DIR"
        echo "--> Pushing latest code..."
        
        # --- THIS IS THE FIX ---
        # Add --force to automatically say "yes" to the manifest prompt
        clasp push --force

        DEPLOYMENT_ID=$(jq -r '.deploymentId' deployment.json)
        if [ -n "$DEPLOYMENT_ID" ] && [ "$DEPLOYMENT_ID" != "null" ]; then
            echo "--> Deploying project to ID: $DEPLOYMENT_ID"
            DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
            clasp deploy --deploymentId "$DEPLOYMENT_ID" --description "$MESSAGE" # Uses your commit message!
        else
            echo -e "--> \033[0;33mWARNING: No deploymentId found. Skipping deployment.\033[0m"
        fi
    )
done < <(find . -name "deployment.json")

echo -e "\n\033[0;32m✅ All processes complete.\033[0m"
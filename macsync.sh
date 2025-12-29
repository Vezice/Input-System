#!/bin/bash

# --- CONFIGURATION ---
# IMPORTANT: The order of files in TARGET_FILES must EXACTLY match the order in SOURCE_FILES.

# 1. List all the TARGET filenames that will be searched for and replaced.
TARGET_FILES=(
    "Phase 2 Importing.js"
    "Phase 2 Sheet Setup.js"
    "Phase 2 Validation.js"
    "Phase 2 Trigger Setup.js"
    "Phase 2 Category-based Importing.js"
    "Phase 3 Slack Features.js"
    "Phase 3 Worker Setup.js"
    "Phase 3 Worker Copy.js"
    "Phase 3 Worker Management.js"
    "Phase 3 BigQuery Export.js"
)

# 2. List the corresponding SOURCE master files.
SOURCE_FILES=(
    "BA Produk SHO/Worker 1/Phase 2 Importing.js"
    "BA Produk SHO/Worker 1/Phase 2 Sheet Setup.js"
    "BA Produk SHO/Worker 1/Phase 2 Validation.js"
    "BA Produk SHO/Worker 1/Phase 2 Trigger Setup.js"
    "BA Produk SHO/Worker 1/Phase 2 Category-based Importing.js"
    "BA Produk SHO/Worker 1/Phase 3 Slack Features.js"
    "BA Produk SHO/Worker 1/Phase 3 Worker Setup.js"
    "BA Produk SHO/Central/Phase 3 Worker Copy.js"
    "BA Produk SHO/Central/Phase 3 Worker Management.js"
    "BA Produk SHO/Central/Phase 3 BigQuery Export.js"
)
# --- END CONFIGURATION ---


# --- SCRIPT LOGIC ---
echo -e "\033[0;32mStarting batch synchronization...\033[0m"

# Loop through the arrays by index.
for (( i=0; i<${#TARGET_FILES[@]}; i++ )); do
    targetFileName="${TARGET_FILES[$i]}"
    sourceFileName="${SOURCE_FILES[$i]}"

    echo -e "\n\033[0;36mProcessing Target: $targetFileName\033[0m"
    
    # Safety Check: Make sure the source file exists.
    if [ ! -f "$sourceFileName" ]; then
        echo -e "  -> \033[0;31m❌ SKIPPING: Source file '$sourceFileName' not found.\033[0m"
        continue
    fi

    # Find all target files and overwrite them.
    find . -name "$targetFileName" | while read -r file; do
        # Don't copy the file onto itself.
        if [ "$file" != "./$sourceFileName" ]; then
            echo "  -> Syncing to: $file"
            cp "$sourceFileName" "$file"
        fi
    done
done

echo -e "\n\033[0;32m✅ Batch synchronization complete.\033[0m"
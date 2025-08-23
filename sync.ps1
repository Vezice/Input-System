# --- CONFIGURATION ---
# Create a map of all the files you want to sync.
# "Target Filename" = "Source File with New Code"

$syncMap = @{
    "Phase 2 Importing.js"                  = "BA Produk SHO\Worker 1\Phase 2 Importing.js";
    "Phase 2 Sheet Setup.js"                = "BA Produk SHO\Worker 1\Phase 2 Sheet Setup.js";
    "Phase 2 Validation.js"                 = "BA Produk SHO\Worker 1\Phase 2 Validation.js";
    "Phase 2 Trigger Setup.js"              = "BA Produk SHO\Worker 1\Phase 2 Trigger Setup.js";
    "Phase 2 Category-based Importing.js"   = "BA Produk SHO\Worker 1\Phase 2 Category-based Importing.js";
    # "Phase 3 Dashboard.js"                  = "BA Produk SHO\Worker 1\Phase 3 Dashboard.js";
    "Phase 3 Slack Features.js"             = "BA Produk SHO\Worker 1\Phase 3 Slack Features.js";
    # "Phase 3 Slack Setup.js"                = "BA Produk SHO\Worker 1\Phase 3 Slack Setup.js";
    "Phase 3 Worker Setup.js"               = "BA Produk SHO\Worker 1\Phase 3 Worker Setup.js";
    "Phase 3 Slack Worker Copy.js"          = "BA Produk SHO\Central\Phase 3 Slack Worker Copy.js";
    "Phase 3 Worker Copy.js"                = "BA Produk SHO\Central\Phase 3 Worker Copy.js";
    "Phase 3 Worker Management.js"          = "BA Produk SHO\Central\Phase 3 Worker Management.js";
    # Add every other file you want to sync here...
}

# --- END CONFIGURATION ---


# --- SCRIPT LOGIC (No changes needed below) ---
Write-Host "Starting batch synchronization..." -ForegroundColor Green

# Loop through each entry in the sync map.
foreach ($entry in $syncMap.GetEnumerator()) {
    $targetFileName = $entry.Key
    $sourceFileName = $entry.Value

    Write-Host "`nProcessing Target: $targetFileName" -ForegroundColor Cyan
    
    # Safety Check: Make sure the source file exists before trying to use it.
    if (-not (Test-Path ".\$sourceFileName")) {
        Write-Host "  -> ❌ SKIPPING: Source file '$sourceFileName' not found." -ForegroundColor Red
        continue # Move to the next entry in the map.
    }

    # Find all target files and overwrite them with the source content.
    Get-ChildItem -Recurse -Filter $targetFileName | ForEach-Object {
        Write-Host "  -> Syncing to: $($_.FullName)"
        Set-Content -Path $_.FullName -Value (Get-Content ".\$sourceFileName" -Raw)
    }
}

Write-Host "`n✅ Batch synchronization complete." -ForegroundColor Green
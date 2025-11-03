# --- CONFIGURATION ---
# IMPORTANT: The order of files in TARGET_FILES must EXACTLY match the order in SOURCE_FILES.

# 1. List all the TARGET filenames that will be searched for and replaced.
$TARGET_FILES = @(
    "Phase 2 Importing.js"
    "Phase 2 Sheet Setup.js"
    "Phase 2 Validation.js"
    "Phase 2 Trigger Setup.js"
    "Phase 2 Category-based Importing.js"
    "Phase 3 Slack Features.js"
    "Phase 3 Worker Setup.js"
    "Phase 3 Worker Copy.js"
    "Phase 3 Worker Management.js"
)

# 2. List the corresponding SOURCE master files.
# Note: Forward slashes (/) have been converted to backslashes (\) for Windows.
$SOURCE_FILES = @(
    "BA Produk SHO\Worker 1\Phase 2 Importing.js"
    "BA Produk SHO\Worker 1\Phase 2 Sheet Setup.js"
    "BA Produk SHO\Worker 1\Phase 2 Validation.js"
    "BA Produk SHO\Worker 1\Phase 2 Trigger Setup.js"
    "BA Produk SHO\Worker 1\Phase 2 Category-based Importing.js"
    "BA Produk SHO\Worker 1\Phase 3 Slack Features.js"
    "BA Produk SHO\Worker 1\Phase 3 Worker Setup.js"
    "BA Produk SHO\Central\Phase 3 Worker Copy.js"
    "BA Produk SHO\Central\Phase 3 Worker Management.js"
)
# --- END CONFIGURATION ---


# --- SCRIPT LOGIC ---
Write-Host "Starting batch synchronization..." -ForegroundColor Green

# Loop through the arrays by index.
for ($i = 0; $i -lt $TARGET_FILES.Count; $i++) {
    $targetFileName = $TARGET_FILES[$i]
    $sourceFileName = $SOURCE_FILES[$i]

    Write-Host "`nProcessing Target: $targetFileName" -ForegroundColor Cyan
    
    # Safety Check: Make sure the source file exists.
    $sourcePath = ".\$sourceFileName"
    if (-not (Test-Path $sourcePath)) {
        Write-Host "  -> ❌ SKIPPING: Source file '$sourceFileName' not found." -ForegroundColor Red
        continue # Move to the next index.
    }

    # Resolve the full path of the source file to prevent self-copying
    $fullSourcePath = (Resolve-Path $sourcePath).FullName

    # Find all target files and overwrite them.
    Get-ChildItem -Path . -Filter $targetFileName -Recurse | ForEach-Object {
        # Don't copy the file onto itself.
        if ($_.FullName -ne $fullSourcePath) {
            Write-Host "  -> Syncing to: $($_.FullName)"
            # Use Copy-Item instead of Set-Content to be faster
            Copy-Item -Path $sourcePath -Destination $_.FullName -Force
        }
    }
}
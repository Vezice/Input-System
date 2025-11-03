# This script will exit immediately if any command fails.
$ErrorActionPreference = "Stop"

# --- Part 1: Git Commit and Push ---

# Stop the script if no commit message was provided.
if ($args.Count -eq 0) {
    Write-Host "❌ Error: Please provide a commit message." -ForegroundColor Red
    Write-Host "Example: .\deploy.ps1 'Final updates'"
    exit 1
}

# The commit message is the first argument
$MESSAGE = $args[0]

Write-Host "--- Starting Git process ---"
Write-Host "Step 1/3: Staging all files..." -ForegroundColor Cyan
git add .

Write-Host "Step 2/3: Committing with message: ""$MESSAGE""" -ForegroundColor Cyan
git commit -m "$MESSAGE"

Write-Host "Step 3/3: Pushing to GitHub..." -ForegroundColor Cyan
git push

# --- Part 2: Clasp Push and Deploy ---
Write-Host "`nStarting batch push and deploy for all projects..." -ForegroundColor Green

Get-ChildItem -Path . -Filter "deployment.json" -Recurse | ForEach-Object {
    $projectDir = $_.DirectoryName
    Write-Host "`nProcessing project in: $projectDir" -ForegroundColor Cyan
    
    # Temporarily change directory
    Push-Location $projectDir
    
    Write-Host "--> Pushing latest code..."
    clasp push
    
    # Read the deployment.json file
    $deployConfig = Get-Content -Raw -Path "deployment.json" | ConvertFrom-Json
    $deploymentId = $deployConfig.deploymentId

    if ($deploymentId) {
        Write-Host "--> Deploying project to ID: $deploymentId"
        # Use the commit message for the deployment description
        clasp deploy --deploymentId $deploymentId --description "$MESSAGE"
    } else {
        Write-Host "--> WARNING: No deploymentId found. Skipping deployment." -ForegroundColor Yellow
    }
    
    # Return to the original directory
    Pop-Location
}

Write-Host "`n✅ All processes complete." -ForegroundColor Green
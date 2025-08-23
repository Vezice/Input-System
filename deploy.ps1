# Script to Push and Deploy all:
# Find all clasp projects, PUSH the latest code, and then DEPLOY
Get-ChildItem -Path . -Filter "deployment.json" -Recurse | ForEach-Object {
    Push-Location $_.DirectoryName
    
    # Step 1: Push the latest local code to the server
    Write-Host "--> Pushing latest code for project in $($_.DirectoryName)"
    clasp push
    
    # Step 2: Deploy the code that is now on the server
    $deployConfig = Get-Content -Raw -Path "deployment.json" | ConvertFrom-Json
    $deploymentId = $deployConfig.deploymentId

    if ($deploymentId) {
        Write-Host "--> Deploying project to ID: $deploymentId"
        clasp deploy --deploymentId $deploymentId --description "Automated batch deployment on $(Get-Date)"
    } else {
        Write-Host "--> WARNING: No deploymentId found in deployment.json. Skipping deployment."
    }
    
    Pop-Location
}
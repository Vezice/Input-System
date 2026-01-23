# Development Notes for Input System

## Code Sync Workflow

When making changes to Worker or Central files:

1. **Only edit files in `BA Produk SHO/Worker 1/` and `BA Produk SHO/Central/`** - these are the source files
2. Run `./macsync.sh` to propagate changes to all other categories and workers
3. Run `./macdeploy.sh 'commit message'` to deploy all projects

Do NOT manually edit files in other category folders - they will be overwritten by macsync.sh.

## Admin Sheet

The Admin Sheet (`Admin Sheet/` folder) is a **separate project** not covered by macsync.sh. Edit its files directly. Use the fast deploy script when only Admin Sheet changes are needed:

```
./macdeploy-admin.sh 'commit message'
```

This deploys only the Admin Sheet (under a minute), versus `./macdeploy.sh` which deploys all 45+ projects (~15 minutes).

The full `./macdeploy.sh` still includes Admin Sheet in its deployment cycle.

## Important: Always Ask Before Deploying

**NEVER run `./macsync.sh`, `./macdeploy.sh`, or `./macdeploy-admin.sh` without asking the user first.** After making code changes, confirm with the user that they are ready to sync and deploy. This prevents premature deployments when the user has additional changes to request.

## Deployment Safety

Deploying does NOT crash currently running import processes. Google Apps Script continues executing the old code for any in-progress functions. New trigger executions pick up the new code. This means deploys are safe to run anytime, even during active imports.

# Development Notes for Input System

## Code Sync Workflow

When making changes to Worker or Central files:

1. **Only edit files in `BA Produk SHO/Worker 1/` and `BA Produk SHO/Central/`** - these are the source files
2. Run `./macsync.sh` to propagate changes to all other categories and workers
3. Run `./macdeploy.sh 'commit message'` to deploy all projects

Do NOT manually edit files in other category folders - they will be overwritten by macsync.sh.

# CustomerLake Branch Strategy

## Branch Model: Trunk-Based Development

All feature work targets `main` via short-lived feature branches.

| Branch | Purpose | DAB Target | Deploys To |
|---|---|---|---|
| `main` | Trunk — integration branch | `dev` (default) | Dev workspace (on push) |
| `release/*` | Release candidate stabilization | `test` | Test workspace (on push) |
| `tags/v*` | Immutable production releases | `prod` | Prod workspace (on tag) |
| `feature/*` | Agent/developer work branches | `dev` | Validate only (CI) |

## Workflow

1. **Feature branches** (`feature/TASK-CODE-description`)
   * Branch from `main`
   * `databricks bundle validate --target dev` runs on every push (CI)
   * Open PR to `main` when ready
   * PR requires: passing CI + 1 approval

2. **Main branch** (`main`)
   * Merge triggers `databricks bundle deploy --target dev`
   * Auto-deploys all resources to dev workspace
   * All agents work against dev target

3. **Release branches** (`release/A`, `release/B`, `release/C`)
   * Cut from `main` when release scope is complete
   * `databricks bundle deploy --target test` on push
   * QA validation runs against test target
   * Bug fixes cherry-picked from `main`

4. **Production tags** (`v1.0.0`, `v2.0.0`)
   * Tag on release branch when QA passes
   * `databricks bundle deploy --target prod` on tag
   * Immutable — no force pushes

## Naming Conventions

* Feature branches: `feature/INFRA-2-dab-config`, `feature/DATA-3-gold-views`
* Release branches: `release/A-data-foundation`, `release/B-agentic-demo`
* Tags: `vA.1.0`, `vB.1.0`, `vC.1.0` (release letter + semver)

## Agent Branch Rules

* Each agent persona creates branches prefixed with their task code
* @devops reviews and merges all PRs
* @pm approves release branch cuts
* Tagging mandate: all commits must reference task codes in messages

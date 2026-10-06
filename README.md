# customerlake-env-setup

Declarative Automation Bundle for the **CustomerLake Readiness** project.

Catalog: `cdm_tmforum` | Tag: `customerlake_project: customerlake`

## Project Structure

```
customerlake-env-setup/
├── databricks.yml                # Bundle config (dev/test/prod targets)
├── customerlake-app/             # Databricks App — FastAPI + React SPA
│   ├── app.py                    # FastAPI backend (profile search, audience, campaigns, steward)
│   ├── app.yaml                  # App runtime config (uvicorn entrypoint)
│   ├── requirements.txt          # Python dependencies
│   └── static/index.html         # React SPA frontend
├── resources/
│   ├── project.md                # Full project plan
│   ├── BRANCH_STRATEGY.md        # Git branching model
│   ├── BRAND_GUIDELINES.md       # Lakelink/CustomerLake brand system
│   ├── customerlake_gold_pipeline.pipeline.yml  # SDP pipeline definition
│   ├── customerlake_refresh.job.yml             # Refresh job definition
│   └── customerlake_app.app.yml                 # Databricks App definition
├── src/
│   ├── pipelines/
│   │   ├── gold_views_pipeline     # SDP gold views (customer_profile_360, transaction_fact) (A-1 — A-4)
│   │   ├── audience_refresh_pipeline  # SDP audience refresh (eligibility → activation) (C-2)
│   │   ├── realtime_profile_updates   # Structured Streaming profile updates (C-3)
│   │   ├── migrate_gold_to_sdp        # Migration utility — manual views → SDP
│   │   └── gold_views.py              # Original scaffold (superseded by gold_views_pipeline)
│   ├── notebooks/
│   │   ├── refresh_metrics.py           # Metric refresh notebook (A-5)
│   │   ├── profile_agent                # CustomerLake Profile Agent (B-1)
│   │   ├── campaign_agent               # CustomerLake Campaign Agent (B-2)
│   │   ├── identity_agent               # Identity Resolution Agent (B-3)
│   │   ├── agent_eval                   # MLflow GenAI agent evaluation (B-4)
│   │   ├── churn_prediction_model       # Churn prediction model (C-5)
│   │   ├── probabilistic_identity_model # Probabilistic identity matching (C-6)
│   │   ├── ltv_propensity_models        # LTV & propensity scoring (C-7)
│   │   ├── federation_demo              # Cross-source federation demo
│   │   ├── campaign_optimization_models # Campaign optimization ML
│   │   ├── RISK-SEG-1_v_risk_tiered_audiences  # Risk-tiered audience segmentation
│   │   ├── _sync_root_to_src            # Utility: sync root notebooks to src/
│   │   └── _devops_sync_cleanup         # Utility: post-migration cleanup
│   └── dashboards/                      # Dashboard JSON files (@data-analyst)
├── .github/workflows/
│   ├── ci.yml                    # Validate on push/PR
│   └── deploy.yml                # Deploy on merge/tag
└── .assistant_instructions.md    # Multi-agent coordination config
```

## Quick Start

```bash
# Validate (dev target, default)
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Deploy to test (release branch)
databricks bundle deploy --target test

# Deploy to prod (tagged release only)
databricks bundle deploy --target prod
```

## Targets

| Target | Mode | Branch | Purpose |
|---|---|---|---|
| `dev` | development | `main`, `feature/*` | Active development |
| `test` | development | `release/*` | QA validation |
| `prod` | production | `v*` tags | Production deployment |

## DAB Resources

| Resource | Type | YAML |
|---|---|---|
| `customerlake-gold-pipeline` | SDP Pipeline | `resources/customerlake_gold_pipeline.pipeline.yml` |
| `customerlake-audience-pipeline` | SDP Pipeline | `resources/customerlake_audience_pipeline.pipeline.yml` |
| `customerlake-refresh` | Job (pipeline + metrics) | `resources/customerlake_refresh.job.yml` |
| `customerlake-realtime-streaming` | Job (streaming) | `resources/customerlake_realtime_streaming.job.yml` |
| `customerlake` | Databricks App | `resources/customerlake_app.app.yml` |

## Releases

* **A: Data Foundation** — Gold views, synthetic tables, metric views
* **B: Agentic Demo** — Agent families, Genie space, AI/BI dashboard
* **C: Production Grade** — SDP pipelines, streaming, full app

## Known Issues

* ~~Agent notebooks at project root~~ — **RESOLVED** (2026-09-29): All 10 notebooks migrated to `src/notebooks/`. Root-level copies remain (workspace Git folder limitation — cannot delete programmatically).
* Prod `run_as` uses personal user — needs service principal when provisioned.
* GitHub secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) must be set in repo Settings to activate CI/CD.

See `resources/BRANCH_STRATEGY.md` for branching model.
# Databricks notebook source
# DBTITLE 1,INFRA-SYNC-ROOTS: Sync Root → src/notebooks/
# INFRA-SYNC-ROOTS: Sync authoritative root notebooks to src/notebooks/
# Run this notebook to complete the sync. Root copies are authoritative.
# After sync, root copies can be safely removed.

import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

ROOT = "/Workspace/Users/stephen.hage@databricks.com/customerlake-env-setup"
DST = f"{ROOT}/src/notebooks"

# 5 drifted notebooks (root is authoritative)
sync_list = [
    "campaign_agent",
    "identity_agent",
    "ltv_propensity_models",
    "agent_eval",
    "profile_agent",
]

results = []
for nb_name in sync_list:
    root_path = f"{ROOT}/{nb_name}"
    dst_path = f"{DST}/{nb_name}"
    try:
        exp = w.workspace.export(path=root_path, format=ExportFormat.SOURCE)
        raw = base64.b64decode(exp.content)
        w.workspace.upload(
            path=dst_path,
            content=raw,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        results.append((nb_name, "SYNCED", len(raw)))
    except Exception as e:
        results.append((nb_name, f"FAILED: {e}", 0))

print("=== SYNC RESULTS ===")
for name, status, size in results:
    icon = "✅" if status == "SYNCED" else "❌"
    print(f"  {icon} {name}: {status} ({size:,} bytes)")

# COMMAND ----------

# DBTITLE 1,Cleanup: Remove Root Copies (optional)
# After verifying sync, remove root-level copies to enforce src/notebooks/ as canonical
# Only run this after confirming the sync succeeded above

ROOT = "/Workspace/Users/stephen.hage@databricks.com/customerlake-env-setup"

all_root_notebooks = [
    # Drifted (now synced)
    "campaign_agent",
    "identity_agent",
    "ltv_propensity_models",
    "agent_eval",
    "profile_agent",
    # Previously identical
    "probabilistic_identity_model",
    "churn_prediction_model",
    # Previously root-only (now migrated)
    "federation_demo",
    "RISK-SEG-1_v_risk_tiered_audiences",
    "campaign_optimization_models",
]

for nb_name in all_root_notebooks:
    path = f"{ROOT}/{nb_name}"
    try:
        w.workspace.delete(path=path)
        print(f"  Deleted root copy: {nb_name}")
    except Exception as e:
        print(f"  Skip {nb_name}: {e}")

print("\nRoot cleanup complete. src/notebooks/ is now the single source of truth.")
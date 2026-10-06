# Databricks notebook source
# DBTITLE 1,INFRA-CLEANUP-ROOT: Sync & Cleanup Root Notebooks
# MAGIC %md
# MAGIC # INFRA-CLEANUP-ROOT: Sync & Cleanup Root-Level Notebooks
# MAGIC
# MAGIC **Task:** `INFRA-CLEANUP-ROOT` | **Owner:** `@devops` | **Project:** CustomerLake Readiness  
# MAGIC **Last audit:** 2026-09-30 by @devops
# MAGIC
# MAGIC ## Problem
# MAGIC After `INFRA-NOTEBOOK-MIGRATE` copied 7 notebooks from project root to `src/notebooks/`, other agents continued editing the **root** copies. This caused content drift. Additionally, 2 notebooks were never migrated at all.
# MAGIC
# MAGIC ## Current Drift Analysis (2026-09-30)
# MAGIC
# MAGIC | Category | Notebooks | Root Lines | Src Lines | Action |
# MAGIC |----------|-----------|-----------|----------|--------|
# MAGIC | **Identical** | `churn_prediction_model` | 662 | 662 | Safe to delete root |
# MAGIC | **Identical** | `probabilistic_identity_model` | 690 | 690 | Safe to delete root |
# MAGIC | **Drifted** | `campaign_agent` | 1322 | 719 | Sync root→src, then delete root |
# MAGIC | **Drifted** | `profile_agent` | 911 | 782 | Sync root→src, then delete root |
# MAGIC | **Drifted** | `identity_agent` | 550 | 550 | Sync root→src, then delete root |
# MAGIC | **Drifted** | `ltv_propensity_models` | 691 | 23 | Sync root→src, then delete root |
# MAGIC | **Drifted** | `agent_eval` | 685 | 638 | Sync root→src, then delete root |
# MAGIC | **Never migrated** | `campaign_optimization_models` | 788 | — | Migrate then delete root |
# MAGIC | **Never migrated** | `RISK-SEG-1_v_risk_tiered_audiences` | 255 | — | Migrate then delete root |
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC 1. **Exports** all 9 root notebooks (7 original + 2 newly discovered)
# MAGIC 2. **Imports** (overwrites) them into `src/notebooks/`
# MAGIC 3. **Verifies** all migrated copies match via SHA-256
# MAGIC 4. **Deletes** the 9 root copies
# MAGIC 5. **Validates** DAB config has no stale root references
# MAGIC
# MAGIC ⚠️ **Run this notebook manually** — automated agents cannot perform workspace overwrite/delete operations.

# COMMAND ----------

# DBTITLE 1,Step 1: Export All Root Notebooks
import requests, base64, hashlib

host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

root_path = "/Users/stephen.hage@databricks.com/customerlake-env-setup"
migrated_path = f"{root_path}/src/notebooks"

# 9 notebooks to sync and clean up (7 original + 2 never migrated)
notebooks = [
    "campaign_agent",
    "agent_eval",
    "identity_agent",
    "ltv_propensity_models",
    "churn_prediction_model",
    "probabilistic_identity_model",
    "profile_agent",
    "campaign_optimization_models",
    "RISK-SEG-1_v_risk_tiered_audiences",
]

# Export all root notebooks
exports = {}
for nb in notebooks:
    resp = requests.get(f"{host}/api/2.0/workspace/export",
                       headers=headers,
                       params={"path": f"{root_path}/{nb}", "format": "SOURCE"})
    if resp.status_code == 200:
        content = resp.json()["content"]
        raw = base64.b64decode(content)
        sha = hashlib.sha256(raw).hexdigest()[:12]
        exports[nb] = {"content": content, "size": len(raw), "sha": sha}
        print(f"\u2705 Exported {nb}: {len(raw):,} bytes (SHA: {sha})")
    else:
        print(f"\u274c Export failed for {nb}: {resp.status_code}")

print(f"\n{len(exports)}/{len(notebooks)} notebooks exported to memory.")

# COMMAND ----------

# DBTITLE 1,Step 2: Sync Root → src/notebooks/ (Overwrite)
# Import each exported notebook to src/notebooks/ (overwrite existing)
results = []
for nb, data in exports.items():
    dst = f"{migrated_path}/{nb}"
    resp = requests.post(f"{host}/api/2.0/workspace/import",
                        headers=headers,
                        json={
                            "path": dst,
                            "content": data["content"],
                            "format": "SOURCE",
                            "language": "PYTHON",
                            "overwrite": True
                        })
    if resp.status_code == 200:
        results.append((nb, "\u2705 SYNCED", f"{data['size']:,} bytes"))
        print(f"\u2705 Synced {nb} \u2192 {dst}")
    else:
        results.append((nb, "\u274c FAILED", resp.text[:200]))
        print(f"\u274c Failed {nb}: {resp.text[:200]}")

print(f"\n{sum(1 for _,s,_ in results if 'SYNCED' in s)}/{len(results)} synced")

# COMMAND ----------

# DBTITLE 1,Step 3: Verify Sync (Compare Root vs Migrated)
# Verify migrated copies match root exports
verified = 0
for nb, data in exports.items():
    dst = f"{migrated_path}/{nb}"
    resp = requests.get(f"{host}/api/2.0/workspace/export",
                       headers=headers,
                       params={"path": dst, "format": "SOURCE"})
    if resp.status_code == 200:
        migrated_raw = base64.b64decode(resp.json()["content"])
        migrated_sha = hashlib.sha256(migrated_raw).hexdigest()[:12]
        match = migrated_sha == data["sha"]
        status = "\u2705 MATCH" if match else "\u274c MISMATCH"
        print(f"{status} {nb}: root={data['sha']} migrated={migrated_sha}")
        if match:
            verified += 1
    else:
        print(f"\u274c Could not export migrated {nb}")

print(f"\n{verified}/{len(exports)} verified. ", end="")
if verified == len(exports):
    print("All match! Safe to delete root copies.")
else:
    print("\u26a0\ufe0f Some mismatches. Do NOT delete root copies until resolved.")

# COMMAND ----------

# DBTITLE 1,Step 4: Delete Root Copies (only if Step 3 passes)
# Only run this after Step 3 confirms all matches! (9 notebooks total)
assert verified == len(exports), f"Only {verified}/{len(exports)} verified. Fix mismatches first!"

deleted = 0
for nb in notebooks:
    src = f"{root_path}/{nb}"
    resp = requests.post(f"{host}/api/2.0/workspace/delete",
                        headers=headers,
                        json={"path": src, "recursive": False})
    if resp.status_code == 200:
        deleted += 1
        print(f"\ud83d\uddd1\ufe0f Deleted root: {src}")
    else:
        print(f"\u274c Delete failed for {src}: {resp.text[:200]}")

print(f"\n{deleted}/{len(notebooks)} root notebooks deleted.")
print("\u2705 INFRA-CLEANUP-ROOT complete!")

# COMMAND ----------

# DBTITLE 1,Step 5: Final DAB Validation
# Verify DAB config doesn't reference any root-level notebooks
import yaml

dab_path = f"{root_path}/databricks.yml"
resp = requests.get(f"{host}/api/2.0/workspace/export",
                   headers=headers,
                   params={"path": dab_path, "format": "AUTO"})

if resp.status_code == 200:
    content = base64.b64decode(resp.json()["content"]).decode("utf-8")
    # Check for any root-level notebook references
    root_refs = [nb for nb in notebooks if f"/{nb}" in content and "/src/notebooks/" not in content.split(f"/{nb}")[0][-20:]]
    if root_refs:
        print(f"\u26a0\ufe0f DAB config still references root notebooks: {root_refs}")
    else:
        print("\u2705 DAB config clean — no root-level notebook references")

# Also check resource YAML files
for resource in ["customerlake_refresh.job.yml", "customerlake_gold_pipeline.pipeline.yml", "customerlake_audience_pipeline.pipeline.yml"]:
    rpath = f"{root_path}/resources/{resource}"
    resp = requests.get(f"{host}/api/2.0/workspace/export",
                       headers=headers,
                       params={"path": rpath, "format": "AUTO"})
    if resp.status_code == 200:
        rcontent = base64.b64decode(resp.json()["content"]).decode("utf-8")
        if "src/notebooks/" in rcontent:
            print(f"\u2705 {resource}: references src/notebooks/ (correct)")
        else:
            print(f"\u26a0\ufe0f {resource}: may need path update")

print("\n\u2705 Validation complete. Task INFRA-CLEANUP-ROOT is done.")
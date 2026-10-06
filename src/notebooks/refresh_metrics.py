# Databricks notebook source
# MAGIC %md
# MAGIC # CustomerLake Metric Refresh
# MAGIC
# MAGIC Owner: @data-analyst | Tag: `customerlake_project: customerlake`
# MAGIC
# MAGIC Runs after the gold pipeline completes. Refreshes all `_metrics.customerlake_*`
# MAGIC metric views by forcing a re-read of their backing tables.
# MAGIC
# MAGIC Called by the `customerlake-refresh` job (task: `refresh_metrics`).

# COMMAND ----------

# TODO(@data-analyst): Implement metric view refresh logic
# This notebook should:
#   1. List all metric views in _metrics schema matching customerlake_*
#   2. Force-refresh each by running a trivial query against it
#   3. Log refresh timestamps and row counts
#   4. Raise an alert if any metric view is stale or empty

catalog = "cdm_tmforum"
metrics_schema = "_metrics"
prefix = "customerlake_"

print(f"[placeholder] Would refresh {catalog}.{metrics_schema}.{prefix}* metric views")
print("Awaiting @data-analyst to implement Task A-5")

# Databricks notebook source
# DBTITLE 1,Migrate Gold Objects to SDP Pipeline Ownership
# CustomerLake Gold → SDP Migration
# Task: C-1 prerequisite
# Owner: @data-engineer
#
# This notebook transfers ownership of gold-layer objects to the
# customerlake_gold_pipeline SDP pipeline.
#
# WHAT IT DOES:
# 1. Verifies gold.digital_activity_source backup exists (75K rows)
# 2. Drops existing VIEW gold.customer_profile_360
# 3. Drops existing VIEW gold.transaction_fact
# 4. Drops existing TABLE gold.digital_activity
# 5. The SDP pipeline then creates these as pipeline-managed MVs
#
# SAFE TO RE-RUN: All operations are idempotent (IF EXISTS).

CAT = "cdm_tmforum"

# Step 1: Verify backup
backup_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CAT}.gold.digital_activity_source").collect()[0].cnt
assert backup_count >= 75000, f"Backup has only {backup_count} rows — expected >=75K"
print(f"✅ Backup verified: gold.digital_activity_source has {backup_count:,} rows")

# Step 2: Drop existing views (no data loss — these are computed views)
spark.sql(f"DROP VIEW IF EXISTS {CAT}.gold.customer_profile_360")
print("✅ Dropped VIEW gold.customer_profile_360")

spark.sql(f"DROP VIEW IF EXISTS {CAT}.gold.transaction_fact")
print("✅ Dropped VIEW gold.transaction_fact")

# Step 3: Drop existing table (data backed up in step 1)
spark.sql(f"DROP TABLE IF EXISTS {CAT}.gold.digital_activity")
print("✅ Dropped TABLE gold.digital_activity (backup in digital_activity_source)")

# Step 4: Verify clean state
remaining = spark.sql(f"""
    SELECT table_name, table_type 
    FROM {CAT}.information_schema.tables 
    WHERE table_schema = 'gold' 
    AND table_name IN ('customer_profile_360', 'transaction_fact', 'digital_activity')
""").collect()
assert len(remaining) == 0, f"Objects still exist: {remaining}"
print("\n✅ Gold schema ready for SDP pipeline ownership")
print("   Next: Run the customerlake_gold_pipeline to create materialized views")
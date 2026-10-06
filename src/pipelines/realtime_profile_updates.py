# Databricks notebook source
# DBTITLE 1,C-3: Real-Time Profile Updates — Structured Streaming
# MAGIC %md
# MAGIC # C-3: Real-Time Profile Updates — Structured Streaming
# MAGIC
# MAGIC **Task:** Process new interactions, billing events, and digital activity into `gold.profile_realtime_metrics` — a low-latency aggregate table that complements `customer_profile_360`.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - Three parallel Structured Streaming queries reading Delta tables as streams
# MAGIC - Each uses `foreachBatch` to MERGE aggregates into `gold.profile_realtime_metrics`
# MAGIC - Entity resolution join: `interaction.customer_id` → `tmf_customer.customer.party_id` → `identity.entity_xref` → `entity_id`
# MAGIC - `transaction_fact` and `digital_activity` already carry `entity_id`
# MAGIC
# MAGIC **Target:** `cdm_tmforum.gold.profile_realtime_metrics` (13 columns, one row per entity_id)
# MAGIC
# MAGIC **Tag:** `customerlake_project: customerlake`

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
CATALOG = "cdm_tmforum"
TARGET_TABLE = f"{CATALOG}.gold.profile_realtime_metrics"
CHECKPOINT_BASE = "/Volumes/cdm_tmforum/gold/_checkpoints/realtime_profile"

# Streaming config
MAX_FILES_PER_TRIGGER = 100  # Controls micro-batch size
TRIGGER_INTERVAL = "30 seconds"  # Processing interval

print(f"Target: {TARGET_TABLE}")
print(f"Checkpoint: {CHECKPOINT_BASE}")

# COMMAND ----------

# DBTITLE 1,Entity Resolution Lookup (broadcast)
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Entity resolution lookup: customer_id -> entity_id
# In cdm_tmforum, customer_id maps directly to entity_xref.source_record_id
# (source_instance='TMF_PARTY'). This is broadcast-joined into interaction
# and billing streams for identity-resolved aggregation.
entity_lookup = (
    spark.table(f"{CATALOG}.identity.entity_xref")
    .filter("source_instance = 'TMF_PARTY' AND is_current = true")
    .select(
        F.col("source_record_id").alias("customer_id"),  # matches tmf_customer IDs
        F.col("entity_id")
    )
    .distinct()
)

entity_lookup.cache()
print(f"Entity lookup: {entity_lookup.count()} customer_id -> entity_id mappings")

# COMMAND ----------

# DBTITLE 1,Stream 1: Interaction Metrics
def merge_interaction_metrics(batch_df, batch_id):
    """Process a micro-batch of interactions into profile_realtime_metrics.
    
    Joins interactions to entity_id via broadcast entity lookup,
    then MERGEs aggregated metrics (last_interaction_date, count_90d, avg_csat)
    into the target table.
    """
    if batch_df.isEmpty():
        return
    
    # Resolve interactions to entity_id via broadcast join
    resolved = (
        batch_df
        .join(
            F.broadcast(entity_lookup),
            batch_df["customer_id"].cast("string") == entity_lookup["customer_id"],
            "inner"
        )
        .select(
            entity_lookup["entity_id"],
            batch_df["created_timestamp"],
            batch_df["customer_satisfaction_score"]
        )
    )
    
    # Aggregate per entity_id
    agg = (
        resolved
        .groupBy("entity_id")
        .agg(
            F.max("created_timestamp").alias("last_interaction_date"),
            F.count("*").alias("total_interaction_count"),
            F.sum(
                F.when(
                    F.col("created_timestamp") >= F.date_add(F.current_date(), -90), 1
                ).otherwise(0)
            ).alias("interaction_count_90d"),
            # NOTE: customer_satisfaction_score in source contains reference IDs, not scores.
            # Filter to valid CSAT range (0-10) to exclude non-score values.
            F.avg(
                F.when(
                    F.col("customer_satisfaction_score").cast("double").between(0, 10),
                    F.col("customer_satisfaction_score").cast("double")
                )
            ).alias("avg_csat_score")
        )
        .withColumn("metrics_updated_at", F.current_timestamp())
    )
    
    # MERGE into target
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("t")
        .merge(agg.alias("s"), "t.entity_id = s.entity_id")
        .whenMatchedUpdate(set={
            "last_interaction_date": F.greatest("t.last_interaction_date", "s.last_interaction_date"),
            "interaction_count_90d": "s.interaction_count_90d",
            "total_interaction_count": F.coalesce("t.total_interaction_count", F.lit(0)) + F.col("s.total_interaction_count"),
            "avg_csat_score": F.coalesce("s.avg_csat_score", "t.avg_csat_score"),
            "metrics_updated_at": "s.metrics_updated_at"
        })
        .whenNotMatchedInsert(values={
            "entity_id": "s.entity_id",
            "last_interaction_date": "s.last_interaction_date",
            "interaction_count_90d": "s.interaction_count_90d",
            "total_interaction_count": "s.total_interaction_count",
            "avg_csat_score": "s.avg_csat_score",
            "metrics_updated_at": "s.metrics_updated_at"
        })
        .execute()
    )
    print(f"  [Interactions] Batch {batch_id}: merged {agg.count()} entity aggregates")


# Start interaction streaming query
interaction_stream = (
    spark.readStream
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .table(f"{CATALOG}.tmf_customer.interaction")
    .select("customer_id", "created_timestamp", "customer_satisfaction_score")
)

interaction_query = (
    interaction_stream.writeStream
    .foreachBatch(merge_interaction_metrics)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/interactions")
    .trigger(availableNow=True)  # Process all available data, then stop
    .queryName("customerlake_interaction_metrics")
    .start()
)

print("Interaction stream started (availableNow mode)")

# COMMAND ----------

# DBTITLE 1,Stream 2: Transaction / Billing Metrics
def merge_transaction_metrics(batch_df, batch_id):
    """Process a micro-batch of billing records into profile_realtime_metrics.
    
    Reads from applied_billing_rate + payment (actual Delta tables, not the
    transaction_fact VIEW which is not streamable). Resolves customer_id to
    entity_id via broadcast entity lookup, then MERGEs LTV aggregates.
    """
    if batch_df.isEmpty():
        return
    
    # Resolve to entity_id
    resolved = (
        batch_df
        .join(
            F.broadcast(entity_lookup),
            batch_df["customer_id"].cast("string") == entity_lookup["customer_id"],
            "inner"
        )
        .select(
            entity_lookup["entity_id"],
            batch_df["txn_amount"],
            batch_df["txn_timestamp"]
        )
    )
    
    agg = (
        resolved
        .filter(F.col("entity_id").isNotNull())
        .groupBy("entity_id")
        .agg(
            F.max("txn_timestamp").alias("last_transaction_date"),
            F.sum("txn_amount").cast("decimal(18,2)").alias("total_ltv"),
            F.count("*").alias("transaction_count")
        )
        .withColumn("metrics_updated_at", F.current_timestamp())
    )
    
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("t")
        .merge(agg.alias("s"), "t.entity_id = s.entity_id")
        .whenMatchedUpdate(set={
            "last_transaction_date": F.greatest("t.last_transaction_date", "s.last_transaction_date"),
            "total_ltv": F.coalesce("t.total_ltv", F.lit(0).cast("decimal(18,2)")) + F.coalesce("s.total_ltv", F.lit(0).cast("decimal(18,2)")),
            "transaction_count": F.coalesce("t.transaction_count", F.lit(0)) + F.col("s.transaction_count"),
            "metrics_updated_at": "s.metrics_updated_at"
        })
        .whenNotMatchedInsert(values={
            "entity_id": "s.entity_id",
            "last_transaction_date": "s.last_transaction_date",
            "total_ltv": "s.total_ltv",
            "transaction_count": "s.transaction_count",
            "metrics_updated_at": "s.metrics_updated_at"
        })
        .execute()
    )
    print(f"  [Transactions] Batch {batch_id}: merged {agg.count()} entity aggregates")


# Stream from actual Delta tables (applied_billing_rate + payment), not the VIEW
# Normalize columns to a common schema before union
billing_stream = (
    spark.readStream
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .table(f"{CATALOG}.tmf_customer.applied_billing_rate")
    .select(
        F.col("customer_id"),
        F.col("applied_amount").alias("txn_amount"),
        F.col("application_timestamp").cast("timestamp").alias("txn_timestamp")
    )
)

payment_stream = (
    spark.readStream
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .table(f"{CATALOG}.tmf_customer.payment")
    .select(
        F.col("customer_id"),
        F.col("amount").alias("txn_amount"),
        F.col("created_timestamp").alias("txn_timestamp")
    )
)

# Union the two billing streams
transaction_stream = billing_stream.union(payment_stream)

transaction_query = (
    transaction_stream.writeStream
    .foreachBatch(merge_transaction_metrics)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/transactions")
    .trigger(availableNow=True)
    .queryName("customerlake_transaction_metrics")
    .start()
)

print("Transaction stream started (billing + payment sources, availableNow mode)")

# COMMAND ----------

# DBTITLE 1,Stream 3: Digital Activity Metrics
def merge_digital_metrics(batch_df, batch_id):
    """Process a micro-batch of digital activity into profile_realtime_metrics.
    
    digital_activity already carries entity_id.
    Aggregates event_count_30d, total_digital_events, conversion_count.
    """
    if batch_df.isEmpty():
        return
    
    agg = (
        batch_df
        .filter(F.col("entity_id").isNotNull())
        .groupBy("entity_id")
        .agg(
            F.max("event_timestamp").alias("last_digital_activity_date"),
            F.sum(
                F.when(
                    F.col("event_timestamp") >= F.date_add(F.current_date(), -30), 1
                ).otherwise(0)
            ).alias("event_count_30d"),
            F.count("*").alias("total_digital_events"),
            F.sum(F.when(F.col("conversion_flag") == True, 1).otherwise(0)).alias("conversion_count")
        )
        .withColumn("metrics_updated_at", F.current_timestamp())
    )
    
    target = DeltaTable.forName(spark, TARGET_TABLE)
    (
        target.alias("t")
        .merge(agg.alias("s"), "t.entity_id = s.entity_id")
        .whenMatchedUpdate(set={
            "last_digital_activity_date": F.greatest("t.last_digital_activity_date", "s.last_digital_activity_date"),
            "event_count_30d": "s.event_count_30d",
            "total_digital_events": F.coalesce("t.total_digital_events", F.lit(0)) + F.col("s.total_digital_events"),
            "conversion_count": F.coalesce("t.conversion_count", F.lit(0)) + F.col("s.conversion_count"),
            "metrics_updated_at": "s.metrics_updated_at"
        })
        .whenNotMatchedInsert(values={
            "entity_id": "s.entity_id",
            "last_digital_activity_date": "s.last_digital_activity_date",
            "event_count_30d": "s.event_count_30d",
            "total_digital_events": "s.total_digital_events",
            "conversion_count": "s.conversion_count",
            "metrics_updated_at": "s.metrics_updated_at"
        })
        .execute()
    )
    print(f"  [Digital] Batch {batch_id}: merged {agg.count()} entity aggregates")


# Start digital activity streaming query
digital_stream = (
    spark.readStream
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .table(f"{CATALOG}.gold.digital_activity")
    .select("entity_id", "event_timestamp", "conversion_flag")
)

digital_query = (
    digital_stream.writeStream
    .foreachBatch(merge_digital_metrics)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/digital_activity")
    .trigger(availableNow=True)
    .queryName("customerlake_digital_metrics")
    .start()
)

print("Digital activity stream started (availableNow mode)")

# COMMAND ----------

# DBTITLE 1,Await All Streams and Validate
# Wait for all three streams to complete their initial load
print("Waiting for all streams to complete...")
interaction_query.awaitTermination()
print("  Interaction stream completed.")
transaction_query.awaitTermination()
print("  Transaction stream completed.")
digital_query.awaitTermination()
print("  Digital activity stream completed.")
print("\nAll streams finished processing available data.")

# COMMAND ----------

# DBTITLE 1,Validation: Row counts and coverage
# MAGIC %sql
# MAGIC -- Validate: profile_realtime_metrics coverage
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_entities,
# MAGIC   COUNT(last_interaction_date) AS has_interactions,
# MAGIC   COUNT(total_ltv) AS has_transactions,
# MAGIC   COUNT(last_digital_activity_date) AS has_digital,
# MAGIC   ROUND(AVG(total_ltv), 2) AS avg_ltv,
# MAGIC   ROUND(AVG(event_count_30d), 1) AS avg_events_30d,
# MAGIC   ROUND(AVG(interaction_count_90d), 1) AS avg_interactions_90d,
# MAGIC   MIN(metrics_updated_at) AS earliest_update,
# MAGIC   MAX(metrics_updated_at) AS latest_update
# MAGIC FROM cdm_tmforum.gold.profile_realtime_metrics

# COMMAND ----------

# DBTITLE 1,Validation: Sample joined with customer_profile_360
# MAGIC %sql
# MAGIC -- Validate: Join realtime metrics with customer_profile_360
# MAGIC SELECT 
# MAGIC   p.entity_id,
# MAGIC   p.name,
# MAGIC   p.lifecycle_status,
# MAGIC   r.last_interaction_date AS rt_last_interaction,
# MAGIC   p.last_interaction_date AS view_last_interaction,
# MAGIC   r.total_ltv AS rt_total_ltv,
# MAGIC   p.total_billed_amount AS view_total_billed,
# MAGIC   r.event_count_30d AS rt_events_30d,
# MAGIC   r.conversion_count AS rt_conversions,
# MAGIC   r.metrics_updated_at
# MAGIC FROM cdm_tmforum.gold.customer_profile_360 p
# MAGIC JOIN cdm_tmforum.gold.profile_realtime_metrics r ON p.entity_id = r.entity_id
# MAGIC WHERE p.lifecycle_status = 'active'
# MAGIC ORDER BY r.total_ltv DESC NULLS LAST
# MAGIC LIMIT 10
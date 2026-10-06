# Databricks notebook source
# DBTITLE 1,Pipeline Configuration
# CustomerLake Audience Refresh — Lakeflow Spark Declarative Pipeline
# Owner: @data-engineer | Task: C-2
# Tag: customerlake_project: customerlake
#
# Incremental audience refresh pipeline:
#   eligibility → consent → deduplication → activation
#
# Materializes (in marketing_sdp schema):
#   - eligible_contacts        (private MV — intermediate eligible contact pool)
#   - audience_snapshot_refresh (MV — current deduplicated audience, one row per entity × purpose × channel)
#   - activation_queue          (MV — activation records ready for channel push)
#
# Each pipeline refresh recomputes from the latest marketing_eligibility +
# consent_event data, producing a new snapshot_timestamp watermark.
# The pipeline-managed marketing_sdp schema preserves each refresh state.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("customerlake_catalog", "cdm_tmforum")

# COMMAND ----------

# DBTITLE 1,MV 1: eligible_contacts (private intermediate)
@dp.materialized_view(
    name="eligible_contacts",
    comment=(
        "Private intermediate: all contacts passing eligibility evaluation. "
        "Filters marketing.marketing_eligibility to is_eligible=TRUE only. "
        "Includes consent flags, channel validation, and community context. "
        "Not published to catalog — consumed only by downstream pipeline MVs."
    ),
    table_properties={"customerlake_project": "customerlake"},
    private=True,
)
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect_or_drop("is_eligible", "is_eligible = true")
@dp.expect("has_contact_address", "contact_address IS NOT NULL")
def eligible_contacts():
    return spark.sql(f"""
    SELECT
        eligibility_id,
        entity_id,
        party_id,
        organization_entity_id,
        account_id,
        contact_id,
        person_name,
        email,
        contact_medium_id,
        channel_type,
        contact_address,
        purpose,
        jurisdiction,
        is_eligible,
        has_opt_in,
        has_dnc_suppression,
        is_channel_valid,
        is_channel_verified,
        contact_status,
        is_active_role,
        evaluation_timestamp,
        community_id,
        community_size,
        community_type
    FROM {CAT}.marketing.marketing_eligibility
    WHERE is_eligible = true
      AND contact_address IS NOT NULL
    """)

# COMMAND ----------

# DBTITLE 1,MV 2: audience_snapshot_refresh (deduplicated audience)
@dp.materialized_view(
    name="audience_snapshot_refresh",
    comment=(
        "Current deduplicated audience snapshot. One row per entity × purpose × "
        "activatable channel. Derives audience_name from purpose + channel_type. "
        "Deduplication: when an entity has multiple eligible contacts for the "
        "same purpose + channel, keeps the contact with the highest community_size "
        "(corporate family reach), then alphabetical contact_address as tiebreaker. "
        "Each pipeline refresh produces a new snapshot_timestamp watermark. "
        "Schema-compatible with marketing.audience_snapshot for downstream merge."
    ),
    table_properties={"customerlake_project": "customerlake"},
)
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("valid_audience_name", "audience_name IS NOT NULL")
@dp.expect("valid_contact_address", "contact_address IS NOT NULL")
@dp.expect("is_deduplicated", "is_deduplicated = true")
def audience_snapshot_refresh():
    return spark.sql(f"""
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY entity_id, purpose, channel_type
                ORDER BY community_size DESC NULLS LAST, contact_address ASC
            ) AS rn
        FROM eligible_contacts
    ),
    deduplicated AS (
        SELECT * FROM ranked WHERE rn = 1
    ),
    audience_counts AS (
        SELECT purpose, channel_type, COUNT(*) AS total_eligible
        FROM deduplicated
        GROUP BY purpose, channel_type
    )
    SELECT
        uuid() AS snapshot_id,
        current_timestamp() AS snapshot_timestamp,
        UPPER(CONCAT(d.purpose, '_', d.channel_type)) AS audience_name,
        UPPER(d.purpose) AS purpose,
        d.channel_type,
        d.entity_id,
        d.party_id,
        d.organization_entity_id,
        d.contact_address,
        d.eligibility_id,
        true AS is_deduplicated,
        CASE
            WHEN d.community_size > 1 THEN 'HIGHEST_COMMUNITY_REACH'
            ELSE 'FIRST_CONTACT_ALPHABETICAL'
        END AS dedup_reason,
        ac.total_eligible,
        'PIPELINE' AS origin,
        CAST(current_timestamp() AS STRING) AS simulation_run_id
    FROM deduplicated d
    JOIN audience_counts ac
      ON d.purpose = ac.purpose AND d.channel_type = ac.channel_type
    """)

# COMMAND ----------

# DBTITLE 1,MV 3: activation_queue (channel-routed activation records)
@dp.materialized_view(
    name="activation_queue",
    comment=(
        "Activation records ready for channel push. Each audience member is "
        "routed to a destination_system based on channel_type using deterministic "
        "assignment (hash of entity_id for reproducibility). Maps channels to "
        "systems: email→Braze/HubSpot, paid_social→Google_Ads/Meta, "
        "crm_list/telephone/fax→Salesforce_Campaign/HubSpot, sms→Twilio, "
        "push_notification→Braze. Delivery_status set to 'pending_push' — "
        "downstream activation process updates to final status. "
        "Schema-compatible with marketing.activation_log for downstream merge."
    ),
    table_properties={"customerlake_project": "customerlake"},
)
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("valid_destination", "destination_system IS NOT NULL")
@dp.expect("valid_destination_type", "destination_type IS NOT NULL")
@dp.expect("valid_status", "delivery_status IN ('pending_push','delivered','bounced','matched','unmatched','suppressed')")
def activation_queue():
    return spark.sql(f"""
    SELECT
        uuid() AS activation_id,
        a.snapshot_id AS audience_snapshot_id,
        a.audience_name,
        a.entity_id,
        CAST(a.party_id AS STRING) AS party_id,
        a.contact_address,

        -- Deterministic destination_system routing based on channel + entity hash
        CASE a.channel_type
            WHEN 'email' THEN
                CASE WHEN abs(hash(a.entity_id)) % 4 < 3 THEN 'Braze' ELSE 'HubSpot' END
            WHEN 'social_media' THEN
                CASE WHEN abs(hash(a.entity_id)) % 5 < 3 THEN 'Google_Ads' ELSE 'Meta_Custom_Audience' END
            WHEN 'telephone' THEN 'Salesforce_Campaign'
            WHEN 'fax' THEN 'Salesforce_Campaign'
            WHEN 'mobile' THEN 'Twilio'
            WHEN 'postal' THEN 'Salesforce_Campaign'
            WHEN 'web_portal' THEN 'HubSpot'
            WHEN 'instant_messaging' THEN 'Braze'
            ELSE 'Salesforce_Campaign'
        END AS destination_system,

        -- Map channel_type to activation destination_type
        CASE a.channel_type
            WHEN 'email' THEN 'email'
            WHEN 'social_media' THEN 'paid_social'
            WHEN 'telephone' THEN 'crm_list'
            WHEN 'fax' THEN 'crm_list'
            WHEN 'mobile' THEN 'sms'
            WHEN 'postal' THEN 'crm_list'
            WHEN 'web_portal' THEN 'push_notification'
            WHEN 'instant_messaging' THEN 'push_notification'
            ELSE 'crm_list'
        END AS destination_type,

        current_timestamp() AS pushed_at,
        'pending_push' AS delivery_status,
        CAST(NULL AS DOUBLE) AS match_rate,
        CAST(NULL AS INT) AS impression_count,
        CAST(NULL AS INT) AS click_count,
        CAST(0.0 AS DECIMAL(10,2)) AS cost_amount,
        'USD' AS cost_currency,
        UPPER(a.purpose) AS purpose,
        CAST(NULL AS BOOLEAN) AS conversion_outcome,
        CAST(NULL AS TIMESTAMP) AS conversion_timestamp,
        CAST(NULL AS DECIMAL(10,2)) AS attributed_revenue,
        CAST(NULL AS STRING) AS attribution_model,
        'PIPELINE' AS origin,
        a.simulation_run_id
    FROM audience_snapshot_refresh a
    """)
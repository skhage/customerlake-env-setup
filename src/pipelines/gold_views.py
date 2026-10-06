# CustomerLake Gold Views — Lakeflow Spark Declarative Pipeline
# Owner: @data-engineer | Task: C-1
# Tag: customerlake_project: customerlake
#
# Materializes (as Materialized Views with data quality expectations):
#   - gold.customer_profile_360  (A-1 logic, materialized for scheduled refresh)
#   - gold.transaction_fact      (A-2 logic, materialized for scheduled refresh)
#   - gold.digital_activity      (A-3 synthetic data, materialized with validation)
#
# Prerequisites:
#   Before first pipeline run, drop existing non-pipeline objects:
#     DROP VIEW IF EXISTS cdm_tmforum.gold.customer_profile_360;
#     DROP VIEW IF EXISTS cdm_tmforum.gold.transaction_fact;
#     DROP TABLE IF EXISTS cdm_tmforum.gold.digital_activity;
#   Backup: gold.digital_activity_source (75K rows) already created.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CAT = spark.conf.get("customerlake_catalog", "cdm_tmforum")

# ---------------------------------------------------------------------------
# MV 1: customer_profile_360 — one row per resolved entity
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="customer_profile_360",
    comment=(
        "Unified customer profile — one row per resolved entity. "
        "Merges party demographics (TMF + Salesforce), identity resolution "
        "metadata, account status, installed base summary, interaction "
        "recency, billing summary, and consent flags. Non-customer entities "
        "receive derived lifecycle/segment values. Consent logic uses "
        "channel-specific email DNC from consent_event, falling back to "
        "party privacy flags. CustomerLake foundation view."
    ),
    table_properties={"customerlake_project": "customerlake"},
)
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("name_completeness", "name IS NOT NULL")
@dp.expect("lifecycle_populated", "lifecycle_status IS NOT NULL")
@dp.expect("consent_populated", "consent_marketing IS NOT NULL")
def customer_profile_360():
    return spark.sql(f"""
    WITH
    entity_base AS (
      SELECT entity_id, entity_type
      FROM {CAT}.identity.entity_registry
      WHERE status = 'active'
    ),
    party_xref AS (
      SELECT entity_id, CAST(source_record_id AS BIGINT) AS party_id
      FROM {CAT}.identity.entity_xref
      WHERE source_instance = 'TMF_PARTY' AND is_current = true
    ),
    sf_contact_xref AS (
      SELECT entity_id, source_record_id AS sf_contact_id
      FROM {CAT}.identity.entity_xref
      WHERE source_instance = 'SALESFORCE' AND object_type = 'contact' AND is_current = true
    ),
    source_lifecycle AS (
      SELECT
        entity_id,
        CASE
          WHEN MAX(CASE WHEN source_instance = 'TMF_PARTY' THEN 1 ELSE 0 END) = 1 THEN 'prospect'
          WHEN MAX(CASE WHEN source_instance = 'SALESFORCE' AND object_type = 'contact' THEN 1 ELSE 0 END) = 1 THEN 'prospect'
          WHEN MAX(CASE WHEN source_instance = 'SALESFORCE' AND object_type = 'partner_account' THEN 1 ELSE 0 END) = 1 THEN 'partner'
          WHEN MAX(CASE WHEN source_instance = 'MOCK_MARKETING' THEN 1 ELSE 0 END) = 1 THEN 'lead'
          WHEN MAX(CASE WHEN source_instance = 'MOCK_DNB' THEN 1 ELSE 0 END) = 1 THEN 'known_entity'
          ELSE 'unclassified'
        END AS derived_lifecycle,
        CASE
          WHEN MAX(CASE WHEN source_instance = 'SALESFORCE' AND object_type = 'partner_account' THEN 1 ELSE 0 END) = 1 THEN 'partner'
          WHEN MAX(CASE WHEN source_instance = 'MOCK_DNB' THEN 1 ELSE 0 END) = 1 THEN 'enterprise'
          WHEN MAX(CASE WHEN source_instance = 'SALESFORCE' AND object_type = 'contact' THEN 1 ELSE 0 END) = 1 THEN 'prospect'
          WHEN MAX(CASE WHEN source_instance = 'MOCK_MARKETING' THEN 1 ELSE 0 END) = 1 THEN 'prospect'
          ELSE 'unknown'
        END AS derived_segment,
        CASE
          WHEN MAX(CASE WHEN source_instance = 'SALESFORCE' AND object_type = 'partner_account' THEN 1 ELSE 0 END) = 1 THEN 'partner_active'
          WHEN MAX(CASE WHEN source_instance = 'SALESFORCE' AND object_type = 'contact' THEN 1 ELSE 0 END) = 1 THEN 'crm_managed'
          WHEN MAX(CASE WHEN source_instance = 'MOCK_DNB' THEN 1 ELSE 0 END) = 1 THEN 'data_enriched'
          ELSE 'resolved_only'
        END AS derived_account_status
      FROM {CAT}.identity.entity_xref
      WHERE is_current = true
      GROUP BY entity_id
    ),
    identity_meta AS (
      SELECT entity_id,
        COUNT(DISTINCT source_instance) AS source_count,
        COUNT(*) AS xref_count,
        AVG(confidence) AS resolution_confidence,
        CONCAT_WS(', ', COLLECT_SET(match_rule)) AS match_rule_summary
      FROM {CAT}.identity.entity_xref
      WHERE is_current = true
      GROUP BY entity_id
    ),
    installed_summary AS (
      SELECT entity_id,
        COUNT(CASE WHEN service_status = 'ACTIVE' THEN 1 END) AS active_service_count,
        COUNT(*) AS total_service_count,
        CONCAT_WS(', ', COLLECT_SET(COALESCE(product_type, product_name))) AS product_types
      FROM {CAT}.gold.installed_base
      GROUP BY entity_id
    ),
    interaction_summary AS (
      SELECT customer_id,
        MAX(created_timestamp) AS last_interaction_date,
        SUM(CASE WHEN created_timestamp >= DATE_ADD(CURRENT_DATE(), -90) THEN 1 ELSE 0 END) AS interaction_count_90d,
        AVG(TRY_CAST(customer_satisfaction_score AS DOUBLE)) AS avg_csat_score
      FROM {CAT}.tmf_customer.interaction
      GROUP BY customer_id
    ),
    problem_count AS (
      SELECT customer_id, COUNT(*) AS open_problem_count
      FROM {CAT}.tmf_customer.customer_problem
      WHERE state IN ('submitted','acknowledged','assigned','in_progress','pending_customer','pending_vendor','reopened')
      GROUP BY customer_id
    ),
    billing_summary AS (
      SELECT customer_id, SUM(total_amount) AS total_billed_amount
      FROM {CAT}.tmf_customer.bill
      GROUP BY customer_id
    ),
    payment_summary AS (
      SELECT customer_id, SUM(amount) AS total_paid_amount
      FROM {CAT}.tmf_customer.payment
      GROUP BY customer_id
    ),
    consent_agg AS (
      SELECT party_id,
        BOOL_OR(CASE WHEN channel_type = 'email' AND opt_in_flag = true THEN true ELSE false END) AS email_consent_marketing,
        BOOL_OR(CASE WHEN channel_type = 'email' AND do_not_contact_flag = true THEN true ELSE false END) AS email_dnc,
        BOOL_OR(CASE WHEN opt_in_flag = true THEN true ELSE false END) AS any_consent_profiling,
        BOOL_AND(CASE WHEN channel_type IN ('email', 'telephone', 'mobile') THEN do_not_contact_flag ELSE false END) AS all_primary_dnc
      FROM {CAT}.marketing.consent_event
      GROUP BY party_id
    )
    SELECT
      eb.entity_id, eb.entity_type,
      CAST(px.party_id AS STRING) AS party_id,
      CAST(c.customer_id AS STRING) AS customer_id,
      COALESCE(p.name, CONCAT_WS(' ', sf.FirstName, sf.LastName)) AS name,
      COALESCE(p.family_name, sf.LastName) AS family_name,
      COALESCE(p.given_name, sf.FirstName) AS given_name,
      p.organization_legal_name,
      COALESCE(p.customer_segment, sl.derived_segment) AS customer_segment,
      COALESCE(p.country_of_residence, sf.MailingCountry) AS country_of_residence,
      p.preferred_language,
      p.preferred_contact_method,
      COALESCE(p.primary_email, sf.Email) AS primary_email,
      COALESCE(p.primary_phone, sf.Phone) AS primary_phone,
      p.mobile_phone,
      COALESCE(c.lifecycle_status, sl.derived_lifecycle) AS lifecycle_status,
      COALESCE(c.account_status, sl.derived_account_status) AS account_status,
      c.activation_date, c.churn_date,
      ROUND(CASE
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'churned' THEN 0.85 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.15
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'deceased' THEN 0.80 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.10
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'dormant' THEN 0.65 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.15
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'suspended' THEN 0.55 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.15
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'win_back' THEN 0.45 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.15
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'merged' THEN 0.40 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.10
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'prospect' THEN 0.25 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.15
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'lead' THEN 0.15 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.15
        WHEN COALESCE(c.lifecycle_status, sl.derived_lifecycle) = 'active' THEN 0.05 + (CAST(c.churn_risk_score AS DOUBLE) / 100000) * 0.20
        WHEN c.churn_risk_score IS NOT NULL THEN c.churn_risk_score / 100000
        ELSE NULL
      END, 4) AS churn_risk_score,
      CASE
        WHEN bs.total_billed_amount IS NULL OR bs.total_billed_amount <= 0 THEN NULL
        WHEN bs.total_billed_amount >= 158978 THEN 'very_high'
        WHEN bs.total_billed_amount >= 104687 THEN 'high'
        WHEN bs.total_billed_amount >= 71847 THEN 'medium'
        WHEN bs.total_billed_amount >= 38553 THEN 'low'
        ELSE 'very_low'
      END AS cltv_score_bucket,
      c.arpu_tier, c.credit_class,
      COALESCE(p.vip_indicator, c.vip_flag, false) AS vip_flag,
      im.resolution_confidence,
      COALESCE(im.source_count, 0) AS source_count,
      COALESCE(im.xref_count, 0) AS xref_count,
      im.match_rule_summary,
      COALESCE(isb.active_service_count, 0) AS active_service_count,
      COALESCE(isb.total_service_count, 0) AS total_service_count,
      isb.product_types,
      ints.last_interaction_date,
      COALESCE(ints.interaction_count_90d, 0) AS interaction_count_90d,
      ints.avg_csat_score,
      COALESCE(pc.open_problem_count, 0) AS open_problem_count,
      CAST(COALESCE(bs.total_billed_amount, 0) AS DECIMAL(18,2)) AS total_billed_amount,
      CAST(COALESCE(ps.total_paid_amount, 0) AS DECIMAL(18,2)) AS total_paid_amount,
      CAST(COALESCE(bs.total_billed_amount, 0) - COALESCE(ps.total_paid_amount, 0) AS DECIMAL(18,2)) AS outstanding_balance,
      c.billing_currency,
      COALESCE(ca.email_consent_marketing, p.privacy_consent_marketing, false) AS consent_marketing,
      COALESCE(ca.any_consent_profiling, p.privacy_consent_profiling, false) AS consent_profiling,
      COALESCE(p.privacy_consent_data_sharing, false) AS consent_data_sharing,
      COALESCE(ca.email_dnc, p.do_not_contact_indicator, false) AS do_not_contact,
      CAST(NULL AS STRING) AS publication_id,
      CURRENT_TIMESTAMP() AS snapshot_timestamp
    FROM entity_base eb
    LEFT JOIN party_xref px ON eb.entity_id = px.entity_id
    LEFT JOIN sf_contact_xref scx ON eb.entity_id = scx.entity_id
    LEFT JOIN {CAT}.tmf_shared.party p ON px.party_id = p.party_id
    LEFT JOIN {CAT}.salesforce_source.contact sf ON scx.sf_contact_id = sf.Id
    LEFT JOIN {CAT}.tmf_customer.customer c ON px.party_id = c.customer_id
    LEFT JOIN source_lifecycle sl ON eb.entity_id = sl.entity_id
    LEFT JOIN identity_meta im ON eb.entity_id = im.entity_id
    LEFT JOIN installed_summary isb ON TRY_CAST(eb.entity_id AS BIGINT) = isb.entity_id
    LEFT JOIN interaction_summary ints ON c.customer_id = ints.customer_id
    LEFT JOIN problem_count pc ON c.customer_id = pc.customer_id
    LEFT JOIN billing_summary bs ON c.customer_id = bs.customer_id
    LEFT JOIN payment_summary ps ON c.customer_id = ps.customer_id
    LEFT JOIN consent_agg ca ON px.party_id = ca.party_id
    """)


# ---------------------------------------------------------------------------
# MV 2: transaction_fact — one row per billing event joined to entity_id
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="transaction_fact",
    comment=(
        "Transaction fact view — one row per billing event. Unions "
        "applied_billing_rate (tax/regulatory), fee_charge (charges/admin "
        "fees), bill_adjustment, and payment through identity crosswalk to "
        "entity_id. All 7 transaction types: charge, credit, payment, "
        "adjustment, fee, rebate, tax. CustomerLake transaction domain."
    ),
    table_properties={"customerlake_project": "customerlake"},
)
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("valid_amount", "amount IS NOT NULL")
@dp.expect("valid_transaction_type", "transaction_type IN ('charge','credit','payment','adjustment','fee','rebate','tax')")
@dp.expect("valid_transaction_date", "transaction_date IS NOT NULL")
def transaction_fact():
    return spark.sql(f"""
    WITH
    customer_entity_map AS (
      SELECT CAST(source_record_id AS BIGINT) AS customer_id, entity_id
      FROM {CAT}.identity.entity_xref
      WHERE source_instance = 'TMF_PARTY' AND is_current = true
    ),
    billing_events AS (
      SELECT
        CAST(applied_billing_rate_id AS STRING) AS transaction_id,
        customer_id, billing_account_id,
        CASE
          WHEN status IN ('reversed', 'cancelled', 'rejected') THEN 'credit'
          WHEN reversal_indicator = 'true' AND status = 'adjusted' THEN 'rebate'
          WHEN reversal_indicator = 'true' THEN 'rebate'
          ELSE 'tax'
        END AS transaction_type,
        COALESCE(tax_type, 'recurring') AS transaction_subtype,
        applied_amount AS amount,
        currency_code,
        TRY_CAST(billing_period_start_date AS DATE) AS billing_period_start,
        TRY_CAST(billing_period_end_date AS DATE) AS billing_period_end,
        TRY_CAST(application_timestamp AS TIMESTAMP) AS transaction_date,
        CAST(product_id AS STRING) AS product_reference,
        CAST(service_id AS STRING) AS service_reference,
        rating_source_system AS rating_source,
        'tmf_customer.applied_billing_rate' AS source_table
      FROM {CAT}.tmf_customer.applied_billing_rate
    ),
    payment_events AS (
      SELECT
        CAST(payment_id AS STRING) AS transaction_id,
        customer_id, billing_account_id,
        'payment' AS transaction_type,
        method_type AS transaction_subtype,
        amount, currency_code,
        CAST(NULL AS DATE) AS billing_period_start,
        CAST(NULL AS DATE) AS billing_period_end,
        TRY_CAST(created_timestamp AS TIMESTAMP) AS transaction_date,
        CAST(NULL AS STRING) AS product_reference,
        CAST(NULL AS STRING) AS service_reference,
        CAST(NULL AS STRING) AS rating_source,
        'tmf_customer.payment' AS source_table
      FROM {CAT}.tmf_customer.payment
    ),
    adjustment_events AS (
      SELECT
        CAST(bill_adjustment_id AS STRING) AS transaction_id,
        CAST(party_id AS BIGINT) AS customer_id,
        billing_account_id,
        'adjustment' AS transaction_type,
        adjustment_type AS transaction_subtype,
        adjustment_amount AS amount,
        adjustment_amount_currency AS currency_code,
        CAST(NULL AS DATE) AS billing_period_start,
        CAST(NULL AS DATE) AS billing_period_end,
        TRY_CAST(applied_timestamp AS TIMESTAMP) AS transaction_date,
        CAST(NULL AS STRING) AS product_reference,
        CAST(NULL AS STRING) AS service_reference,
        CAST(NULL AS STRING) AS rating_source,
        'tmf_customer.bill_adjustment' AS source_table
      FROM {CAT}.tmf_customer.bill_adjustment
    ),
    fee_events AS (
      SELECT
        CAST(fee_charge_id AS STRING) AS transaction_id,
        customer_id, billing_account_id,
        CASE
          WHEN trigger_event_type IN ('CONTRACT_BREACH', 'ACCOUNT_SUSPEND', 'CREDIT_LIMIT_EXCEED', 'SERVICE_RECONNECT', 'PAYMENT_OVERDUE') THEN 'charge'
          ELSE 'fee'
        END AS transaction_type,
        trigger_event_type AS transaction_subtype,
        fee_amount AS amount,
        currency_code,
        CAST(NULL AS DATE) AS billing_period_start,
        CAST(NULL AS DATE) AS billing_period_end,
        created_timestamp AS transaction_date,
        CAST(NULL AS STRING) AS product_reference,
        CAST(service_id AS STRING) AS service_reference,
        CAST(NULL AS STRING) AS rating_source,
        'tmf_customer.fee_charge' AS source_table
      FROM {CAT}.tmf_customer.fee_charge
    ),
    all_transactions AS (
      SELECT * FROM billing_events
      UNION ALL SELECT * FROM payment_events
      UNION ALL SELECT * FROM adjustment_events
      UNION ALL SELECT * FROM fee_events
    )
    SELECT
      t.transaction_id,
      cem.entity_id,
      CAST(t.customer_id AS STRING) AS customer_id,
      CAST(t.billing_account_id AS STRING) AS billing_account_id,
      t.transaction_type, t.transaction_subtype,
      t.amount, t.currency_code,
      t.billing_period_start, t.billing_period_end,
      t.transaction_date,
      t.product_reference, t.service_reference,
      t.rating_source, t.source_table
    FROM all_transactions t
    LEFT JOIN customer_entity_map cem ON t.customer_id = cem.customer_id
    """)


# ---------------------------------------------------------------------------
# MV 3: digital_activity — synthetic behavioral events with validation
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="digital_activity",
    comment=(
        "Synthetic digital behavioral events (web/app clickstream). "
        "75K rows keyed to identity.entity_registry.entity_id. "
        "Materialized from gold.digital_activity_source with data quality "
        "expectations applied. CustomerLake interactions domain."
    ),
    table_properties={"customerlake_project": "customerlake"},
)
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("valid_event_type", "event_type IS NOT NULL")
@dp.expect("valid_timestamp", "event_timestamp IS NOT NULL")
@dp.expect("valid_channel", "channel IN ('web', 'mobile_app', 'self_service_portal', 'ivr')")
@dp.expect("valid_device", "device_type IN ('desktop', 'mobile', 'tablet')")
def digital_activity():
    return spark.read.table(f"{CAT}.gold.digital_activity_source")

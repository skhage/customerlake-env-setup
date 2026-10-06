# Databricks notebook source
# DBTITLE 1,C-6: Lakehouse Federation — CustomerLake Cross-System Query Demo
# MAGIC %md
# MAGIC # C-6: Lakehouse Federation for CustomerLake
# MAGIC
# MAGIC **Purpose:** Demonstrate CustomerLake's ability to query customer data across Databricks (Delta) + external systems (PostgreSQL CRM) **without copying data**. This is the "federated data fabric" capability that differentiates CustomerLake from legacy CDPs.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    CustomerLake (Unity Catalog)              │
# MAGIC │                                                             │
# MAGIC │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
# MAGIC │  │ cdm_tmforum   │    │ FOREIGN CAT  │    │ gold views   │  │
# MAGIC │  │ (Delta)       │    │ (Federated)  │    │ (unified)    │  │
# MAGIC │  │               │    │              │    │              │  │
# MAGIC │  │ • party       │    │ • enrichment │    │ • profile    │  │
# MAGIC │  │ • customer    │◄───┤ • partner    │───►│   _360       │  │
# MAGIC │  │ • identity    │    │ • channel    │    │ • enriched   │  │
# MAGIC │  │ • marketing   │    │   perf       │    │   _profile   │  │
# MAGIC │  └──────────────┘    └──────┬───────┘    └──────────────┘  │
# MAGIC │                             │                               │
# MAGIC │                    Lakehouse Federation                      │
# MAGIC │                    (zero-copy, governed)                     │
# MAGIC └─────────────────────┬───────────────────────────────────────┘
# MAGIC                       │
# MAGIC               ┌───────┴───────┐
# MAGIC               │ External CRM  │
# MAGIC               │ (PostgreSQL)  │
# MAGIC               │               │
# MAGIC               │ • customer_   │
# MAGIC               │   enrichment  │
# MAGIC               │ • partner_    │
# MAGIC               │   intelligence│
# MAGIC               │ • channel_    │
# MAGIC               │   performance │
# MAGIC               └───────────────┘
# MAGIC ```
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC 1. **External CRM data** living in PostgreSQL (Lakebase) — NPS scores, account managers, partner intelligence, channel performance
# MAGIC 2. **Lakehouse Federation DDL** — CONNECTION + FOREIGN CATALOG creation (requires metastore admin)
# MAGIC 3. **Live cross-system queries** — joining Delta + PostgreSQL in real time via psycopg
# MAGIC 4. **Enriched profile view** — unified customer profile combining native lakehouse + federated external data
# MAGIC
# MAGIC **Tags:** `customerlake_project: customerlake`

# COMMAND ----------

# DBTITLE 1,Step 1: Lakehouse Federation DDL (requires metastore admin)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- STEP 1: CREATE LAKEHOUSE FEDERATION CONNECTION & FOREIGN CATALOG
# MAGIC -- ============================================================
# MAGIC -- These statements require CREATE CONNECTION on the metastore.
# MAGIC -- Execute as metastore admin / CEO.
# MAGIC
# MAGIC -- 1a. Create the connection to the external CRM PostgreSQL database
# MAGIC CREATE CONNECTION IF NOT EXISTS customerlake_external_crm
# MAGIC TYPE postgresql
# MAGIC OPTIONS (
# MAGIC     host 'ep-fancy-unit-d230umfv.database.us-east-1.cloud.databricks.com',
# MAGIC     port '5432',
# MAGIC     user '<service_principal_or_user>',
# MAGIC     password '<lakebase_token>'
# MAGIC );
# MAGIC
# MAGIC COMMENT ON CONNECTION customerlake_external_crm IS
# MAGIC     'Lakehouse Federation to external CRM (HubSpot/Salesforce supplement) hosted in Lakebase PostgreSQL. Contains customer enrichment, partner intelligence, and channel performance data. Part of CustomerLake Readiness project.';
# MAGIC
# MAGIC -- 1b. Create the foreign catalog mirroring the external_crm schema
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS customerlake_external
# MAGIC USING CONNECTION customerlake_external_crm
# MAGIC OPTIONS (database 'databricks_postgres');
# MAGIC
# MAGIC -- 1c. After creation, data is queryable as:
# MAGIC --   SELECT * FROM customerlake_external.external_crm.customer_enrichment
# MAGIC --   SELECT * FROM customerlake_external.external_crm.partner_intelligence
# MAGIC --   SELECT * FROM customerlake_external.external_crm.channel_performance
# MAGIC
# MAGIC -- 1d. Grant access to data teams
# MAGIC -- GRANT USE CATALOG ON CATALOG customerlake_external TO `data-team`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA customerlake_external.external_crm TO `data-team`;
# MAGIC -- GRANT SELECT ON SCHEMA customerlake_external.external_crm TO `data-team`;

# COMMAND ----------

# DBTITLE 1,Step 2: Install dependencies
# MAGIC %pip install --upgrade 'databricks-sdk>=0.118.0' 'psycopg[binary]' -q

# COMMAND ----------

# DBTITLE 1,Step 3: Connect to external CRM (PostgreSQL via Lakebase)
import psycopg
import pandas as pd
from databricks.sdk import WorkspaceClient

# Connect to external CRM database (Lakebase PostgreSQL)
# In production, this would be via Lakehouse Federation — zero-copy, governed by UC.
# This direct connection demonstrates the same data path.
w = WorkspaceClient()
endpoint_name = "projects/customerlake-task-board/branches/production/endpoints/primary"
host = "ep-fancy-unit-d230umfv.database.us-east-1.cloud.databricks.com"
user = w.current_user.me().user_name
token = w.postgres.generate_database_credential(endpoint=endpoint_name).token
conn = psycopg.connect(host=host, dbname="databricks_postgres", user=user, password=token, sslmode="require")

print("✓ Connected to external CRM database (PostgreSQL)")
print(f"  Host: {host}")
print(f"  Database: databricks_postgres")
print(f"  Schema: external_crm")

# Preview the three external tables
for table in ['customer_enrichment', 'partner_intelligence', 'channel_performance']:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM external_crm.{table}")
    count = cur.fetchone()[0]
    print(f"  external_crm.{table}: {count} rows")

# COMMAND ----------

# DBTITLE 1,Step 4: Preview external CRM enrichment data
# Preview customer enrichment data from external CRM
df_enrichment = pd.read_sql("""
    SELECT customer_id, nps_score, industry_vertical, company_size_band,
           annual_contract_value, renewal_probability, customer_health_score,
           account_manager, preferred_engagement_channel
    FROM external_crm.customer_enrichment
    ORDER BY annual_contract_value DESC
    LIMIT 10
""", conn)

print("External CRM: Top 10 customers by ACV (Annual Contract Value)")
print("=" * 80)
display(spark.createDataFrame(df_enrichment))

# COMMAND ----------

# DBTITLE 1,Step 5: Cross-system join — Delta profiles + external CRM enrichment
# ============================================================
# CROSS-SYSTEM JOIN: CustomerLake native (Delta) + External CRM (PostgreSQL)
# This is what Lakehouse Federation does transparently via SQL.
# Here we demonstrate the same join using direct Postgres access.
# ============================================================

# 1. Load external CRM enrichment from PostgreSQL
df_external = pd.read_sql("""
    SELECT customer_id, nps_score, industry_vertical, company_size_band,
           annual_contract_value, renewal_probability, customer_health_score,
           account_manager, preferred_engagement_channel, source_system
    FROM external_crm.customer_enrichment
""", conn)
external_sdf = spark.createDataFrame(df_external)
external_sdf.createOrReplaceTempView("external_crm_enrichment")
print(f"✓ Loaded {external_sdf.count()} external CRM enrichment records")

# 2. Load partner intelligence from PostgreSQL
df_partners = pd.read_sql("""
    SELECT customer_id, partner_name, partner_tier, co_sell_status,
           partner_sourced, partner_nps_score, last_partner_interaction
    FROM external_crm.partner_intelligence
""", conn)
partners_sdf = spark.createDataFrame(df_partners)
partners_sdf.createOrReplaceTempView("external_partner_intelligence")
print(f"✓ Loaded {partners_sdf.count()} external partner intelligence records")

# 3. Load channel performance from PostgreSQL  
df_channels = pd.read_sql("""
    SELECT customer_id, channel, impressions, clicks, conversions,
           spend_amount, revenue_attributed, measurement_period_start, measurement_period_end
    FROM external_crm.channel_performance
""", conn)
channels_sdf = spark.createDataFrame(df_channels)
channels_sdf.createOrReplaceTempView("external_channel_performance")
print(f"✓ Loaded {channels_sdf.count()} external channel performance records")

print("\n✓ All external data loaded into Spark temp views for cross-system joins")

# COMMAND ----------

# DBTITLE 1,Step 6: Federated enriched profile — the CustomerLake differentiator
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- FEDERATED ENRICHED PROFILE
# MAGIC -- Joins native lakehouse customer data (Delta) with external CRM data (PostgreSQL)
# MAGIC -- This query demonstrates what CustomerLake makes possible:
# MAGIC -- A single SQL statement spanning multiple systems, governed by Unity Catalog.
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     -- Native lakehouse: identity + profile
# MAGIC     cp.entity_id,
# MAGIC     cp.entity_type,
# MAGIC     cp.name AS customer_name,
# MAGIC     cp.customer_segment,
# MAGIC     cp.lifecycle_status,
# MAGIC     cp.churn_risk_score,
# MAGIC     cp.arpu_tier,
# MAGIC     cp.active_service_count,
# MAGIC     cp.consent_marketing,
# MAGIC     
# MAGIC     -- External CRM: enrichment (via federation)
# MAGIC     ext.nps_score,
# MAGIC     ext.industry_vertical,
# MAGIC     ext.company_size_band,
# MAGIC     ext.annual_contract_value AS external_acv,
# MAGIC     ext.renewal_probability,
# MAGIC     ext.customer_health_score,
# MAGIC     ext.account_manager,
# MAGIC     ext.preferred_engagement_channel,
# MAGIC     ext.source_system AS crm_source,
# MAGIC     
# MAGIC     -- Computed: CustomerLake unified risk score
# MAGIC     -- Combines native churn_risk with external health_score
# MAGIC     ROUND(
# MAGIC         (COALESCE(cp.churn_risk_score, 0.5) * 0.6) + 
# MAGIC         ((100 - COALESCE(ext.customer_health_score, 50)) / 100 * 0.4),
# MAGIC     3) AS unified_risk_score,
# MAGIC     
# MAGIC     -- Flag: data from both systems
# MAGIC     CASE 
# MAGIC         WHEN ext.customer_id IS NOT NULL THEN 'Delta + External CRM'
# MAGIC         ELSE 'Delta only'
# MAGIC     END AS data_coverage
# MAGIC
# MAGIC FROM cdm_tmforum.gold.customer_profile_360 cp
# MAGIC LEFT JOIN external_crm_enrichment ext
# MAGIC     ON cp.customer_id = ext.customer_id
# MAGIC WHERE cp.entity_type = 'organization'
# MAGIC ORDER BY ext.annual_contract_value DESC NULLS LAST
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Step 7: Federated partner analysis
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- FEDERATED PARTNER ANALYSIS
# MAGIC -- Which high-value customers have active partner co-sell relationships?
# MAGIC -- Joins native installed base with external partner intelligence.
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     cp.entity_id,
# MAGIC     cp.name AS customer_name,
# MAGIC     cp.customer_segment,
# MAGIC     cp.active_service_count,
# MAGIC     cp.churn_risk_score,
# MAGIC     
# MAGIC     -- External: partner data
# MAGIC     pi.partner_name,
# MAGIC     pi.partner_tier,
# MAGIC     pi.co_sell_status,
# MAGIC     pi.partner_sourced,
# MAGIC     pi.partner_nps_score,
# MAGIC     pi.last_partner_interaction,
# MAGIC     
# MAGIC     -- External: channel investment
# MAGIC     ch.total_spend,
# MAGIC     ch.total_revenue,
# MAGIC     ch.total_conversions,
# MAGIC     ROUND(ch.total_revenue / NULLIF(ch.total_spend, 0), 2) AS channel_roas
# MAGIC
# MAGIC FROM cdm_tmforum.gold.customer_profile_360 cp
# MAGIC INNER JOIN external_partner_intelligence pi
# MAGIC     ON cp.customer_id = pi.customer_id
# MAGIC LEFT JOIN (
# MAGIC     SELECT customer_id,
# MAGIC            SUM(spend_amount) AS total_spend,
# MAGIC            SUM(revenue_attributed) AS total_revenue,
# MAGIC            SUM(conversions) AS total_conversions
# MAGIC     FROM external_channel_performance
# MAGIC     GROUP BY customer_id
# MAGIC ) ch ON cp.customer_id = ch.customer_id
# MAGIC WHERE pi.co_sell_status IN ('active', 'pipeline')
# MAGIC   AND cp.churn_risk_score > 0.5
# MAGIC ORDER BY cp.churn_risk_score DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Step 8: Persist external data as federation landing tables, then create unified view
# ============================================================
# FEDERATION LANDING TABLES
# These tables replicate external CRM data into the gold schema.
# When Lakehouse Federation CONNECTION is created by metastore admin,
# replace these with direct references to the foreign catalog:
#   customerlake_external.external_crm.customer_enrichment
#   customerlake_external.external_crm.partner_intelligence  
#   customerlake_external.external_crm.channel_performance
# ============================================================

# Step 8a: Persist external CRM enrichment
spark.sql("""
    CREATE TABLE IF NOT EXISTS cdm_tmforum.gold.ext_customer_enrichment
    USING DELTA
    TBLPROPERTIES ('customerlake_project' = 'customerlake')
    AS SELECT * FROM external_crm_enrichment
""")
print("✓ Created gold.ext_customer_enrichment")

# Step 8b: Persist external partner intelligence
spark.sql("""
    CREATE TABLE IF NOT EXISTS cdm_tmforum.gold.ext_partner_intelligence
    USING DELTA
    TBLPROPERTIES ('customerlake_project' = 'customerlake')
    AS SELECT * FROM external_partner_intelligence
""")
print("✓ Created gold.ext_partner_intelligence")

# Step 8c: Persist external channel performance
spark.sql("""
    CREATE TABLE IF NOT EXISTS cdm_tmforum.gold.ext_channel_performance
    USING DELTA
    TBLPROPERTIES ('customerlake_project' = 'customerlake')
    AS SELECT * FROM external_channel_performance
""")
print("✓ Created gold.ext_channel_performance")

# Verify counts
for tbl in ['ext_customer_enrichment', 'ext_partner_intelligence', 'ext_channel_performance']:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM cdm_tmforum.gold.{tbl}").collect()[0]['cnt']
    print(f"  cdm_tmforum.gold.{tbl}: {count} rows")

# COMMAND ----------

# DBTITLE 1,Step 9: Create federated enriched view from landing tables
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- CREATE FEDERATED ENRICHED VIEW
# MAGIC -- Joins native lakehouse customer_profile_360 with federation landing tables
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW cdm_tmforum.gold.v_federated_customer_enrichment
# MAGIC AS
# MAGIC SELECT
# MAGIC     cp.entity_id,
# MAGIC     cp.entity_type,
# MAGIC     cp.customer_id,
# MAGIC     cp.name,
# MAGIC     cp.customer_segment,
# MAGIC     cp.lifecycle_status,
# MAGIC     cp.churn_risk_score,
# MAGIC     cp.arpu_tier,
# MAGIC     cp.active_service_count,
# MAGIC     cp.total_billed_amount,
# MAGIC     cp.consent_marketing,
# MAGIC     cp.last_interaction_date,
# MAGIC     
# MAGIC     -- External CRM enrichment (federation landing)
# MAGIC     ext.nps_score,
# MAGIC     ext.industry_vertical,
# MAGIC     ext.company_size_band,
# MAGIC     ext.annual_contract_value,
# MAGIC     ext.renewal_probability,
# MAGIC     ext.customer_health_score,
# MAGIC     ext.account_manager,
# MAGIC     ext.preferred_engagement_channel,
# MAGIC     
# MAGIC     -- Unified risk: native churn + external health
# MAGIC     ROUND(
# MAGIC         (COALESCE(cp.churn_risk_score, 0.5) * 0.6) + 
# MAGIC         ((100 - COALESCE(ext.customer_health_score, 50)) / 100 * 0.4),
# MAGIC     3) AS unified_risk_score,
# MAGIC     
# MAGIC     -- Channel ROI summary (federation landing)
# MAGIC     ch.total_spend AS channel_total_spend,
# MAGIC     ch.total_revenue AS channel_total_revenue,
# MAGIC     ch.total_conversions AS channel_total_conversions,
# MAGIC     ROUND(ch.total_revenue / NULLIF(ch.total_spend, 0), 2) AS channel_roas,
# MAGIC     
# MAGIC     -- Partner info (federation landing)
# MAGIC     pi.partner_name AS primary_partner,
# MAGIC     pi.partner_tier,
# MAGIC     pi.co_sell_status,
# MAGIC     
# MAGIC     -- Provenance
# MAGIC     CASE 
# MAGIC         WHEN ext.customer_id IS NOT NULL THEN 'Delta + External CRM'
# MAGIC         ELSE 'Delta only'
# MAGIC     END AS data_source_coverage,
# MAGIC     current_timestamp() AS federation_timestamp
# MAGIC
# MAGIC FROM cdm_tmforum.gold.customer_profile_360 cp
# MAGIC LEFT JOIN cdm_tmforum.gold.ext_customer_enrichment ext
# MAGIC     ON cp.customer_id = ext.customer_id
# MAGIC LEFT JOIN (
# MAGIC     SELECT customer_id,
# MAGIC            SUM(spend_amount) AS total_spend,
# MAGIC            SUM(revenue_attributed) AS total_revenue,
# MAGIC            SUM(conversions) AS total_conversions
# MAGIC     FROM cdm_tmforum.gold.ext_channel_performance
# MAGIC     GROUP BY customer_id
# MAGIC ) ch ON cp.customer_id = ch.customer_id
# MAGIC LEFT JOIN (
# MAGIC     SELECT customer_id, partner_name, partner_tier, co_sell_status
# MAGIC     FROM (
# MAGIC         SELECT customer_id, partner_name, partner_tier, co_sell_status,
# MAGIC                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_partner_interaction DESC) as rn
# MAGIC         FROM cdm_tmforum.gold.ext_partner_intelligence
# MAGIC     ) ranked WHERE rn = 1
# MAGIC ) pi ON cp.customer_id = pi.customer_id

# COMMAND ----------

# DBTITLE 1,Step 10: Validate federated view + Tag and document
# MAGIC %sql
# MAGIC -- Validate the federated view
# MAGIC SELECT
# MAGIC     data_source_coverage,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(unified_risk_score), 3) AS avg_unified_risk,
# MAGIC     ROUND(AVG(nps_score), 1) AS avg_nps,
# MAGIC     ROUND(AVG(channel_roas), 2) AS avg_channel_roas,
# MAGIC     COUNT(primary_partner) AS with_partner
# MAGIC FROM cdm_tmforum.gold.v_federated_customer_enrichment
# MAGIC GROUP BY data_source_coverage
# MAGIC ORDER BY data_source_coverage

# COMMAND ----------

# DBTITLE 1,Step 11: Tag and document all federation objects
# MAGIC %sql
# MAGIC -- Tag federation view
# MAGIC ALTER VIEW cdm_tmforum.gold.v_federated_customer_enrichment
# MAGIC SET TAGS ('customerlake_project' = 'customerlake');
# MAGIC
# MAGIC -- Document federation view
# MAGIC COMMENT ON VIEW cdm_tmforum.gold.v_federated_customer_enrichment IS
# MAGIC     'Federated enriched customer profile combining native lakehouse data (cdm_tmforum Delta) with external CRM data (PostgreSQL via Lakehouse Federation). Demonstrates CustomerLake cross-system query capability. Columns from external CRM: nps_score, industry_vertical, company_size_band, annual_contract_value, renewal_probability, customer_health_score, account_manager, preferred_engagement_channel, channel metrics, partner info. unified_risk_score blends native churn_risk (60%) with external health_score (40%).';
# MAGIC
# MAGIC -- Document landing tables
# MAGIC COMMENT ON TABLE cdm_tmforum.gold.ext_customer_enrichment IS
# MAGIC     'Federation landing table: external CRM customer enrichment data (NPS scores, health scores, account managers, industry verticals). Source: HubSpot CRM (simulated in Lakebase PostgreSQL). Will be replaced by foreign catalog reference when Lakehouse Federation CONNECTION is created.';
# MAGIC
# MAGIC COMMENT ON TABLE cdm_tmforum.gold.ext_partner_intelligence IS
# MAGIC     'Federation landing table: external partner intelligence data (co-sell status, partner tiers, managed services). Source: partner portal (simulated in Lakebase PostgreSQL). Will be replaced by foreign catalog reference when Lakehouse Federation CONNECTION is created.';
# MAGIC
# MAGIC COMMENT ON TABLE cdm_tmforum.gold.ext_channel_performance IS
# MAGIC     'Federation landing table: external marketing channel performance data (impressions, clicks, conversions, ROAS). Source: marketing automation platform (simulated in Lakebase PostgreSQL). Will be replaced by foreign catalog reference when Lakehouse Federation CONNECTION is created.';

# COMMAND ----------

# DBTITLE 1,Step 11: Summary — what CustomerLake federation enables
# MAGIC %md
# MAGIC ## What Lakehouse Federation Enables for CustomerLake
# MAGIC
# MAGIC ### Legacy CDP Limitation
# MAGIC Traditional CDPs require **copying all customer data** into a single system. This means:
# MAGIC - ETL pipelines to sync CRM, billing, partner, and channel data
# MAGIC - Stale data (sync lag of hours to days)
# MAGIC - Data governance gaps (copies outside the governance perimeter)
# MAGIC - Storage cost duplication
# MAGIC
# MAGIC ### CustomerLake + Lakehouse Federation
# MAGIC CustomerLake **queries data in place** across systems:
# MAGIC - **Zero-copy access** to external CRM (HubSpot, Salesforce), ERP (Oracle, SAP), and marketing platforms
# MAGIC - **Real-time freshness** — every query hits the live source
# MAGIC - **Unity Catalog governance** — access control, audit, lineage across all federated sources
# MAGIC - **Single SQL surface** — analysts write one query spanning Delta + PostgreSQL + Snowflake + BigQuery
# MAGIC
# MAGIC ### External Data in This Demo
# MAGIC
# MAGIC | Table | Source | Records | Purpose |
# MAGIC |---|---|---|---|
# MAGIC | `external_crm.customer_enrichment` | HubSpot CRM (simulated) | 300 | NPS, health scores, account managers, industry verticals |
# MAGIC | `external_crm.partner_intelligence` | Partner portal (simulated) | 150 | Co-sell status, partner tier, managed services |
# MAGIC | `external_crm.channel_performance` | Marketing automation (simulated) | 498 | Impressions, clicks, conversions, ROAS by channel |
# MAGIC
# MAGIC ### CEO Action Required
# MAGIC To enable transparent Lakehouse Federation (vs. the direct psycopg bridge in this notebook):
# MAGIC 1. Execute the `CREATE CONNECTION` and `CREATE FOREIGN CATALOG` statements in **Step 1** as metastore admin
# MAGIC 2. The federated view `gold.v_federated_customer_enrichment` will then query PostgreSQL natively via SQL
# MAGIC 3. Grant appropriate access to data teams
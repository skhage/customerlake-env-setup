# Databricks notebook source
# DBTITLE 1,RISK-SEG-1: Overview
# MAGIC %md
# MAGIC # RISK-SEG-1: Build `gold.v_risk_tiered_audiences` Suppression View
# MAGIC
# MAGIC **Task:** RISK-SEG-1 (Critical priority, unblocked)  
# MAGIC **Agent:** @data-engineer  
# MAGIC **Addresses:** CMO-22 — risk-based targeting failure  
# MAGIC
# MAGIC **Logic:**
# MAGIC 1. JOIN `customer_profile_360` + `churn_prediction` + `propensity_scores`
# MAGIC 2. SUPPRESS: `lifecycle_status IN (deceased, merged, lead, prospect)` + DNC + no marketing consent
# MAGIC 3. RISK TIERS: CRITICAL/HIGH get 3-5x campaign volume allocation
# MAGIC 4. OUTPUT: `entity_id`, `risk_tier`, `recommended_cadence`, `suppressed`, `suppression_reason`, plus context columns
# MAGIC 5. COMMENT ON all columns, tag `customerlake_project = customerlake`

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install --upgrade 'databricks-sdk>=0.118.0' psycopg[binary] --quiet

# COMMAND ----------

# DBTITLE 1,Step 1: Create view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cdm_tmforum.gold.v_risk_tiered_audiences
# MAGIC COMMENT 'Risk-tiered audience suppression view for retention campaigns. Joins customer_profile_360 + churn_prediction + propensity_scores. Suppresses deceased/merged/lead/prospect entities and DNC-flagged contacts. CRITICAL/HIGH tiers get 3-5x campaign volume allocation. Addresses CMO-22: retention campaigns must be differentiated by churn risk tier. Tagged customerlake_project=customerlake.'
# MAGIC AS
# MAGIC SELECT 
# MAGIC     -- Entity identification
# MAGIC     cp.entity_id,
# MAGIC     cp.entity_type,
# MAGIC     cp.lifecycle_status,
# MAGIC     cp.name,
# MAGIC     cp.customer_segment,
# MAGIC
# MAGIC     -- Churn model signals
# MAGIC     COALESCE(ch.churn_risk_tier, 'UNSCORED') AS risk_tier,
# MAGIC     ch.ml_churn_probability,
# MAGIC     ch.revenue_at_risk,
# MAGIC
# MAGIC     -- Propensity / value signals
# MAGIC     ps.value_segment,
# MAGIC     ps.predicted_ltv_12m,
# MAGIC
# MAGIC     -- Profile context
# MAGIC     cp.arpu_tier,
# MAGIC     cp.active_service_count,
# MAGIC     cp.total_billed_amount,
# MAGIC     cp.consent_marketing,
# MAGIC     cp.do_not_contact,
# MAGIC
# MAGIC     -- Suppression: lifecycle-based + DNC + consent
# MAGIC     CASE 
# MAGIC         WHEN cp.lifecycle_status IN ('deceased', 'merged', 'lead', 'prospect') THEN TRUE
# MAGIC         WHEN cp.do_not_contact = TRUE THEN TRUE
# MAGIC         WHEN cp.consent_marketing = FALSE THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS suppressed,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN cp.lifecycle_status = 'deceased'  THEN 'Deceased - ineligible for any outreach'
# MAGIC         WHEN cp.lifecycle_status = 'merged'     THEN 'Merged entity - duplicate, use surviving entity'
# MAGIC         WHEN cp.lifecycle_status = 'lead'       THEN 'Lead - not yet customer, ineligible for retention'
# MAGIC         WHEN cp.lifecycle_status = 'prospect'   THEN 'Prospect - not yet customer, ineligible for retention'
# MAGIC         WHEN cp.do_not_contact = TRUE           THEN 'Do-not-contact flag active'
# MAGIC         WHEN cp.consent_marketing = FALSE        THEN 'Marketing consent not granted'
# MAGIC         ELSE NULL
# MAGIC     END AS suppression_reason,
# MAGIC
# MAGIC     -- Recommended cadence: CRITICAL/HIGH = daily, MEDIUM = weekly, LOW/MINIMAL = monthly
# MAGIC     CASE COALESCE(ch.churn_risk_tier, 'UNSCORED')
# MAGIC         WHEN 'CRITICAL' THEN 'daily'
# MAGIC         WHEN 'HIGH'     THEN 'daily'
# MAGIC         WHEN 'MEDIUM'   THEN 'weekly'
# MAGIC         WHEN 'LOW'      THEN 'monthly'
# MAGIC         WHEN 'MINIMAL'  THEN 'monthly'
# MAGIC         ELSE 'monthly'
# MAGIC     END AS recommended_cadence,
# MAGIC
# MAGIC     -- Volume allocation multiplier: CRITICAL=5x, HIGH=3x, MEDIUM=1x, LOW=0.5x, MINIMAL=0.25x
# MAGIC     CASE COALESCE(ch.churn_risk_tier, 'UNSCORED')
# MAGIC         WHEN 'CRITICAL' THEN 5.0
# MAGIC         WHEN 'HIGH'     THEN 3.0
# MAGIC         WHEN 'MEDIUM'   THEN 1.0
# MAGIC         WHEN 'LOW'      THEN 0.5
# MAGIC         WHEN 'MINIMAL'  THEN 0.25
# MAGIC         ELSE 0.25
# MAGIC     END AS volume_allocation_multiplier,
# MAGIC
# MAGIC     -- Campaign Agent action recommendation
# MAGIC     CASE 
# MAGIC         WHEN cp.lifecycle_status IN ('deceased', 'merged', 'lead', 'prospect') 
# MAGIC             THEN 'SUPPRESS'
# MAGIC         WHEN cp.do_not_contact = TRUE OR cp.consent_marketing = FALSE
# MAGIC             THEN 'SUPPRESS'
# MAGIC         WHEN COALESCE(ch.churn_risk_tier, 'UNSCORED') = 'CRITICAL'
# MAGIC             THEN 'URGENT_RETAIN: 5x volume, daily cadence, multi-channel'
# MAGIC         WHEN COALESCE(ch.churn_risk_tier, 'UNSCORED') = 'HIGH'
# MAGIC             THEN 'HIGH_PRIORITY_RETAIN: 3x volume, daily cadence'
# MAGIC         WHEN COALESCE(ch.churn_risk_tier, 'UNSCORED') = 'MEDIUM'
# MAGIC             THEN 'STANDARD_RETAIN: 1x volume, weekly cadence'
# MAGIC         WHEN COALESCE(ch.churn_risk_tier, 'UNSCORED') = 'LOW'
# MAGIC             THEN 'NURTURE: 0.5x volume, monthly cadence'
# MAGIC         WHEN COALESCE(ch.churn_risk_tier, 'UNSCORED') = 'MINIMAL'
# MAGIC             THEN 'MONITOR: 0.25x volume, monthly check-in only'
# MAGIC         ELSE 'EVALUATE: No churn score available'
# MAGIC     END AS campaign_action
# MAGIC
# MAGIC FROM cdm_tmforum.gold.customer_profile_360 cp
# MAGIC LEFT JOIN cdm_tmforum.gold.churn_prediction ch 
# MAGIC     ON cp.entity_id = ch.entity_id
# MAGIC LEFT JOIN cdm_tmforum.gold.propensity_scores ps 
# MAGIC     ON cp.entity_id = ps.entity_id

# COMMAND ----------

# DBTITLE 1,Step 2: Add column-level comments (Semantic Modeling Mandate)
# Column-level descriptions per CEO Semantic Modeling Mandate
v = "cdm_tmforum.gold.v_risk_tiered_audiences"
comments = {
    "entity_id": "FK to identity.entity_registry. Durable resolved entity identifier.",
    "entity_type": "Entity classification: organization or individual.",
    "lifecycle_status": "Customer lifecycle status: active/churned/dormant/suspended/deceased/win_back/merged/prospect/lead/partner.",
    "name": "Coalesced display name from party demographics.",
    "customer_segment": "Business segment: enterprise, mid_market, smb, consumer, government.",
    "risk_tier": "ML churn risk tier: CRITICAL(>=0.8), HIGH(>=0.6), MEDIUM(>=0.4), LOW(>=0.2), MINIMAL(<0.2), UNSCORED if no model output.",
    "ml_churn_probability": "ML-predicted churn probability (0-1). LightGBM on 53 cross-source features.",
    "revenue_at_risk": "churn_probability x total_billed_amount. Key retention prioritization metric.",
    "value_segment": "ML value segment: Prime Target, Protect and Retain, At Risk - Low Value, Nurture.",
    "predicted_ltv_12m": "ML-predicted 12-month lifetime value.",
    "arpu_tier": "Average revenue per user tier from billing system.",
    "active_service_count": "Count of active services from installed_base.",
    "total_billed_amount": "Total lifetime billed amount. Revenue base for at-risk calculation.",
    "consent_marketing": "TRUE if entity has granted marketing consent (channel: email).",
    "do_not_contact": "TRUE if entity has do-not-contact flag from privacy preferences.",
    "suppressed": "TRUE if entity is excluded from all retention campaigns. Reasons: deceased, merged, lead, prospect, DNC, no consent.",
    "suppression_reason": "Human-readable reason for suppression. NULL if entity is eligible for targeting.",
    "recommended_cadence": "Recommended outreach frequency: daily (CRITICAL/HIGH), weekly (MEDIUM), monthly (LOW/MINIMAL).",
    "volume_allocation_multiplier": "Campaign volume multiplier: CRITICAL=5x, HIGH=3x, MEDIUM=1x, LOW=0.5x, MINIMAL=0.25x. Controls send allocation.",
    "campaign_action": "Campaign Agent directive: SUPPRESS, URGENT_RETAIN, HIGH_PRIORITY_RETAIN, STANDARD_RETAIN, NURTURE, MONITOR, or EVALUATE.",
}
for col, desc in comments.items():
    safe_desc = desc.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {v}.{col} IS '{safe_desc}'")
    print(f"  {col}: done")
print(f"\nAll {len(comments)} column comments applied.")

# COMMAND ----------

# DBTITLE 1,Step 3: Apply customerlake_project tag
# MAGIC %sql
# MAGIC ALTER VIEW cdm_tmforum.gold.v_risk_tiered_audiences SET TAGS ('customerlake_project' = 'customerlake');

# COMMAND ----------

# DBTITLE 1,Step 4: Validate view - tier distribution and suppression counts
# MAGIC %sql
# MAGIC -- Validation: tier distribution, suppression rates, and volume allocation
# MAGIC SELECT 
# MAGIC     risk_tier,
# MAGIC     COUNT(*) AS total_entities,
# MAGIC     SUM(CASE WHEN suppressed THEN 1 ELSE 0 END) AS suppressed_count,
# MAGIC     SUM(CASE WHEN NOT suppressed THEN 1 ELSE 0 END) AS targetable_count,
# MAGIC     ROUND(AVG(ml_churn_probability), 3) AS avg_churn_prob,
# MAGIC     ROUND(SUM(revenue_at_risk), 2) AS total_revenue_at_risk,
# MAGIC     recommended_cadence,
# MAGIC     volume_allocation_multiplier
# MAGIC FROM cdm_tmforum.gold.v_risk_tiered_audiences
# MAGIC GROUP BY risk_tier, recommended_cadence, volume_allocation_multiplier
# MAGIC ORDER BY 
# MAGIC     CASE risk_tier 
# MAGIC         WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 
# MAGIC         WHEN 'LOW' THEN 4 WHEN 'MINIMAL' THEN 5 ELSE 6 
# MAGIC     END

# COMMAND ----------

# DBTITLE 1,Step 5: Validate suppression reasons breakdown
# MAGIC %sql
# MAGIC -- Suppression reasons breakdown
# MAGIC SELECT 
# MAGIC     suppression_reason,
# MAGIC     COUNT(*) AS entity_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_suppressed
# MAGIC FROM cdm_tmforum.gold.v_risk_tiered_audiences
# MAGIC WHERE suppressed = TRUE
# MAGIC GROUP BY suppression_reason
# MAGIC ORDER BY entity_count DESC

# COMMAND ----------

# DBTITLE 1,Step 6: Validate campaign_action distribution
# MAGIC %sql
# MAGIC -- Campaign action distribution (what Campaign Agent will see)
# MAGIC SELECT 
# MAGIC     campaign_action,
# MAGIC     COUNT(*) AS entity_count,
# MAGIC     ROUND(AVG(revenue_at_risk), 2) AS avg_revenue_at_risk,
# MAGIC     ROUND(AVG(predicted_ltv_12m), 2) AS avg_predicted_ltv
# MAGIC FROM cdm_tmforum.gold.v_risk_tiered_audiences
# MAGIC GROUP BY campaign_action
# MAGIC ORDER BY entity_count DESC

# COMMAND ----------

# DBTITLE 1,Step 7: Update Lakebase task board - mark RISK-SEG-1 completed
import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
endpoint_name = "projects/customerlake-task-board/branches/production/endpoints/primary"
host = "ep-fancy-unit-d230umfv.database.us-east-1.cloud.databricks.com"
user = w.current_user.me().user_name
token = w.postgres.generate_database_credential(endpoint=endpoint_name).token
conn = psycopg.connect(host=host, dbname="databricks_postgres", user=user, password=token, sslmode="require")
cur = conn.cursor()

status_notes = """COMPLETED by @data-engineer (Run 2026-09-30).

DELIVERED: gold.v_risk_tiered_audiences — 20-column risk-tiered suppression view.

SOURCES: customer_profile_360 + churn_prediction + propensity_scores (3-way LEFT JOIN on entity_id).

SUPPRESSION LOGIC (18,423 entities suppressed):
- Prospect (72.0%%): 13,261 non-customers excluded
- Lead (6.9%%): 1,262 pre-customers excluded  
- Deceased (6.6%%): 1,220 ineligible
- DNC flag (6.2%%): 1,141 do-not-contact
- No consent (5.2%%): 963 no marketing consent
- Merged (3.1%%): 576 duplicate entities

TARGETABLE ENTITIES BY TIER:
- CRITICAL: 2,531 targetable (5x volume, daily cadence) — $195.7M revenue at risk
- HIGH: 1,067 targetable (3x volume, daily cadence) — $71.8M revenue at risk
- MEDIUM: 725 targetable (1x volume, weekly cadence) — $26.9M revenue at risk
- LOW: 254 targetable (0.5x volume, monthly) — $6.3M revenue at risk
- MINIMAL: 33 targetable (0.25x volume, monthly) — $0.7M revenue at risk

COMPLIANCE: All 20 columns have COMMENT ON descriptions. Tag customerlake_project=customerlake applied.
Notebook: customerlake-env-setup/RISK-SEG-1_v_risk_tiered_audiences.

CMO-22 RESOLUTION: View enables Campaign Agent to differentiate retention by risk tier.
CRITICAL/HIGH get 3-5x volume vs. LOW/MINIMAL. Suppression prevents wasted spend."""

cur.execute(
    "UPDATE agent_tasks SET status = 'completed', status_notes = %s, completed_date = NOW(), updated_at = NOW() WHERE task_code = 'RISK-SEG-1'",
    (status_notes,)
)
conn.commit()

cur.execute("SELECT task_code, status, completed_date FROM agent_tasks WHERE task_code = 'RISK-SEG-1'")
row = cur.fetchone()
print(f"Task {row[0]}: status={row[1]}, completed={row[2]}")
conn.close()
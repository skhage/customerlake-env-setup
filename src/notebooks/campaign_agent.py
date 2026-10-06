# Databricks notebook source
# DBTITLE 1,CustomerLake Campaign Agent
# MAGIC %md
# MAGIC # CustomerLake Campaign Agent
# MAGIC
# MAGIC **Purpose:** Demonstrate CustomerLake's Agentic Audience Building — the ability to take a natural language campaign brief, dynamically build a SQL segment across unified profiles, evaluate eligibility and consent in real time, and simulate activation to downstream systems with closed-loop measurement.
# MAGIC
# MAGIC **What legacy CDPs can't do:**
# MAGIC - Legacy CDPs (Braze, Iterable, Salesforce CDP) require marketers to build audiences through point-and-click segment builders with pre-defined attributes — they can't interpret intent.
# MAGIC - Legacy CDPs evaluate eligibility at send time with simple opt-in checks — they can't reason about multi-channel consent, DNC suppression, jurisdiction, and channel validity simultaneously.
# MAGIC - Legacy CDPs can't explain *why* a customer was included or excluded from an audience, or recommend the optimal channel.
# MAGIC - Legacy CDPs treat activation as fire-and-forget — CustomerLake's Campaign Agent closes the loop with attribution, delivery tracking, and ROI measurement.
# MAGIC
# MAGIC **CustomerLake differentiators this agent demonstrates:**
# MAGIC 1. **Natural Language → SQL Segment:** Marketer describes intent; agent writes and validates the query
# MAGIC 2. **Cross-Source Audience Intelligence:** Segment criteria span billing, CRM, service, and behavioral data — unified through identity resolution
# MAGIC 3. **Consent-Aware Activation:** Agent evaluates 193K eligibility records across purpose × channel × jurisdiction before any send
# MAGIC 4. **Closed-Loop Measurement:** Agent checks delivery rates, match rates, and attributed revenue from the activation log
# MAGIC 5. **Explainable Exclusions:** Every suppressed contact gets a reason code (NO_OPT_IN, DNC_SUPPRESSED, INVALID_CHANNEL)
# MAGIC
# MAGIC **Data foundation (from `cdm_tmforum`):**
# MAGIC - `gold.customer_profile_360` — 35.7K unified profiles (45 attributes, 5 source systems)
# MAGIC - `marketing.audience_snapshot` — 55.7K versioned audience memberships (5 purposes, 3 channels)
# MAGIC - `marketing.marketing_eligibility` — 193K eligibility records (consent + channel + role validation)
# MAGIC - `marketing.consent_event` — 38.6K consent records per party × channel
# MAGIC - `marketing.activation_log` — 30K activation records across 6 destination systems
# MAGIC - `marketing.campaign_event` — 18K campaign touches (send, open, click, response, meeting)
# MAGIC - `marketing.campaign_attribution` — 3.7K attributed conversions with revenue allocation

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openai -q

# COMMAND ----------

# DBTITLE 1,Setup — imports and configuration
import json, textwrap, uuid
from datetime import datetime
from openai import OpenAI
from pyspark.sql import functions as F

# --- Configuration ---
CATALOG = "cdm_tmforum"
MODEL_ENDPOINT = "databricks-claude-sonnet-4-5"  # Best reasoning for campaign strategy
MAX_TURNS = 12  # Campaign workflows can require more tool calls

# Foundation Model API client (OpenAI-compatible)
client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url=f"https://{spark.conf.get('spark.databricks.workspaceUrl').rstrip('/')}/serving-endpoints"
)

# Verify data foundation
profile_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.gold.customer_profile_360").collect()[0].cnt
elig_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.marketing.marketing_eligibility").collect()[0].cnt
activation_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.marketing.activation_log").collect()[0].cnt
audience_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.marketing.audience_snapshot").collect()[0].cnt

print(f"Campaign Agent configured")
print(f"  Model: {MODEL_ENDPOINT}")
print(f"  Catalog: {CATALOG}")
print(f"  Profiles: {profile_count:,} unified entities")
print(f"  Eligibility records: {elig_count:,}")
print(f"  Audience memberships: {audience_count:,}")
print(f"  Activation records: {activation_count:,}")
print(f"  Segments: enterprise, medium_business, small_business, government, consumer, wholesale")
print(f"  Purposes: RETENTION, CROSS_SELL, UPSELL, WINBACK, RENEWAL_OUTREACH")
print(f"  Channels: email, telephone, fax")
print(f"  Destinations: Braze, Salesforce_Campaign, Google_Ads, HubSpot, Twilio, Meta_Custom_Audience")

# COMMAND ----------

# DBTITLE 1,Tool definitions — SQL functions the agent calls
# ---------------------------------------------------------------------------
# Campaign Agent tools
# Each tool runs scoped SQL queries across the CustomerLake marketing layer
# and returns structured JSON the agent can reason over.
# ---------------------------------------------------------------------------

def build_segment_query(where_clause: str, limit: int = 100) -> str:
    """Execute a segment query against the unified customer profile.
    where_clause: SQL WHERE conditions (e.g., "customer_segment = 'enterprise' AND churn_risk_score > 50000")
    Returns matching profiles with key attributes for audience evaluation."""
    query = f"""
        SELECT entity_id, entity_type, name, customer_segment, lifecycle_status,
               churn_risk_score, arpu_tier, active_service_count, total_service_count,
               primary_email, primary_phone, consent_marketing, consent_profiling,
               credit_class, vip_flag, last_interaction_date, outstanding_balance,
               total_billed_amount, source_count, resolution_confidence
        FROM {CATALOG}.gold.customer_profile_360
        WHERE {where_clause}
        LIMIT {limit}
    """
    try:
        rows = spark.sql(query).collect()
        return json.dumps({
            "query_executed": query.strip(),
            "match_count": len(rows),
            "profiles": [r.asDict() for r in rows]
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query_attempted": query.strip()})


def get_segment_size(where_clause: str) -> str:
    """Get the count of profiles matching a segment condition without fetching all rows.
    Use this to validate segment size before building the full audience."""
    query = f"""
        SELECT COUNT(*) as total_matches,
               COUNT(CASE WHEN consent_marketing = true THEN 1 END) as consent_marketing_yes,
               COUNT(CASE WHEN lifecycle_status = 'active' THEN 1 END) as active_count,
               COUNT(CASE WHEN lifecycle_status = 'churned' THEN 1 END) as churned_count,
               ROUND(AVG(churn_risk_score), 2) as avg_churn_risk,
               COUNT(DISTINCT customer_segment) as segment_diversity
        FROM {CATALOG}.gold.customer_profile_360
        WHERE {where_clause}
    """
    try:
        row = spark.sql(query).collect()[0]
        return json.dumps(row.asDict(), default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query_attempted": query.strip()})


def evaluate_eligibility(entity_ids: str, purpose: str, channel: str = "email") -> str:
    """Check marketing eligibility for a list of entity_ids.
    entity_ids: comma-separated entity IDs (e.g., '10001,10002,10003')
    purpose: RETENTION, CROSS_SELL, UPSELL, WINBACK, or RENEWAL_OUTREACH
    channel: email, telephone, or fax
    Returns eligibility status with exclusion reasons."""
    id_list = entity_ids.replace(" ", "")
    query = f"""
        SELECT entity_id, person_name, email, contact_address, channel_type,
               is_eligible, exclusion_reason, has_opt_in, has_dnc_suppression,
               is_channel_valid, is_channel_verified, is_active_role
        FROM {CATALOG}.marketing.marketing_eligibility
        WHERE entity_id IN ({id_list})
          AND purpose = '{purpose}'
          AND channel_type = '{channel}'
    """
    rows = spark.sql(query).collect()
    eligible = [r for r in rows if r.is_eligible]
    excluded = [r for r in rows if not r.is_eligible]
    return json.dumps({
        "purpose": purpose,
        "channel": channel,
        "total_evaluated": len(rows),
        "eligible_count": len(eligible),
        "excluded_count": len(excluded),
        "exclusion_breakdown": {},
        "eligible": [r.asDict() for r in eligible[:20]],
        "excluded_sample": [{"entity_id": r.entity_id, "reason": r.exclusion_reason, 
                             "has_opt_in": r.has_opt_in, "has_dnc": r.has_dnc_suppression}
                            for r in excluded[:10]]
    }, default=str)


def check_consent_status(entity_ids: str, channel: str = "email") -> str:
    """Check detailed consent records for entities on a specific channel.
    Returns opt-in flags, DNC status, validity, and verification per entity."""
    id_list = entity_ids.replace(" ", "")
    query = f"""
        SELECT ce.party_id, ce.channel_type, ce.contact_address,
               ce.opt_in_flag, ce.do_not_contact_flag, ce.valid_flag,
               ce.verified_flag, ce.status, ce.valid_from, ce.valid_to
        FROM {CATALOG}.marketing.consent_event ce
        WHERE ce.party_id IN ({id_list})
          AND ce.channel_type = '{channel}'
    """
    rows = spark.sql(query).collect()
    return json.dumps({
        "channel": channel,
        "records": [r.asDict() for r in rows[:30]],
        "summary": {
            "total": len(rows),
            "opted_in": sum(1 for r in rows if r.opt_in_flag),
            "dnc_flagged": sum(1 for r in rows if r.do_not_contact_flag),
            "valid": sum(1 for r in rows if r.valid_flag),
            "verified": sum(1 for r in rows if r.verified_flag)
        }
    }, default=str)


def get_existing_audiences(purpose_filter: str = None) -> str:
    """Browse existing audience snapshots with membership counts.
    purpose_filter: optional filter (RETENTION, CROSS_SELL, UPSELL, WINBACK, RENEWAL_OUTREACH)"""
    where = ""
    if purpose_filter:
        where = f"WHERE purpose = '{purpose_filter}'"
    query = f"""
        SELECT audience_name, purpose, channel_type, 
               COUNT(*) as member_count,
               COUNT(CASE WHEN is_deduplicated = true THEN 1 END) as deduped_count,
               MIN(snapshot_timestamp) as first_snapshot,
               MAX(snapshot_timestamp) as last_snapshot
        FROM {CATALOG}.marketing.audience_snapshot
        {where}
        GROUP BY audience_name, purpose, channel_type
        ORDER BY member_count DESC
    """
    rows = spark.sql(query).collect()
    return json.dumps([r.asDict() for r in rows], default=str)


def get_campaign_performance(purpose: str = None, channel: str = None) -> str:
    """Get historical campaign performance metrics.
    Returns send/open/click/response/conversion rates by campaign."""
    where_parts = ["1=1"]
    if purpose:
        where_parts.append(f"purpose = '{purpose}'")
    if channel:
        where_parts.append(f"channel_type = '{channel}'")
    where = " AND ".join(where_parts)
    query = f"""
        SELECT campaign_name, channel_type, purpose,
               SUM(total_sends) as sends, SUM(total_opens) as opens,
               SUM(total_clicks) as clicks, SUM(total_responses) as responses,
               SUM(total_conversions) as conversions,
               ROUND(SUM(conversion_amount), 2) as total_revenue,
               ROUND(SUM(total_opens) * 100.0 / NULLIF(SUM(total_sends), 0), 1) as open_rate,
               ROUND(SUM(total_clicks) * 100.0 / NULLIF(SUM(total_sends), 0), 1) as click_rate,
               ROUND(SUM(total_conversions) * 100.0 / NULLIF(SUM(total_sends), 0), 1) as conversion_rate
        FROM {CATALOG}.marketing.campaign_measurement
        WHERE {where}
        GROUP BY campaign_name, channel_type, purpose
        ORDER BY total_revenue DESC
        LIMIT 15
    """
    rows = spark.sql(query).collect()
    return json.dumps([r.asDict() for r in rows], default=str)


def get_campaign_attribution(purpose: str = None, limit: int = 20) -> str:
    """Get campaign attribution data showing which touches drove conversions and revenue.
    Shows last-touch attribution with conversion amounts and days-to-convert."""
    where = f"WHERE purpose = '{purpose}'" if purpose else ""
    query = f"""
        SELECT channel_type, purpose, attribution_model,
               COUNT(*) as attributions,
               ROUND(SUM(attributed_amount), 2) as total_attributed_revenue,
               ROUND(AVG(attributed_amount), 2) as avg_attributed_revenue,
               ROUND(AVG(days_before_conversion), 1) as avg_days_to_convert,
               ROUND(AVG(attribution_weight), 3) as avg_attribution_weight
        FROM {CATALOG}.marketing.campaign_attribution
        {where}
        GROUP BY channel_type, purpose, attribution_model
        ORDER BY total_attributed_revenue DESC
        LIMIT {limit}
    """
    rows = spark.sql(query).collect()
    return json.dumps([r.asDict() for r in rows], default=str)


def get_activation_delivery_stats(audience_name: str = None, destination: str = None) -> str:
    """Check activation delivery statistics from the activation log.
    Shows delivery rates, match rates, and conversion outcomes by destination."""
    where_parts = ["1=1"]
    if audience_name:
        where_parts.append(f"audience_name = '{audience_name}'")
    if destination:
        where_parts.append(f"destination_system = '{destination}'")
    where = " AND ".join(where_parts)
    query = f"""
        SELECT destination_system, destination_type, audience_name, delivery_status,
               COUNT(*) as records,
               ROUND(AVG(match_rate), 3) as avg_match_rate,
               SUM(CASE WHEN conversion_outcome = true THEN 1 ELSE 0 END) as conversions,
               ROUND(SUM(COALESCE(attributed_revenue, 0)), 2) as total_revenue,
               ROUND(SUM(COALESCE(cost_amount, 0)), 2) as total_cost
        FROM {CATALOG}.marketing.activation_log
        WHERE {where}
        GROUP BY destination_system, destination_type, audience_name, delivery_status
        ORDER BY records DESC
        LIMIT 25
    """
    rows = spark.sql(query).collect()
    return json.dumps([r.asDict() for r in rows], default=str)


def simulate_activation_push(entity_ids: str, destination_system: str, destination_type: str, audience_name: str) -> str:
    """Simulate pushing an audience to a destination system.
    Does NOT write data — generates a simulated activation manifest showing what would happen.
    destination_system: Braze, Salesforce_Campaign, Google_Ads, HubSpot, Twilio, Meta_Custom_Audience
    destination_type: email, sms, paid_social, crm_list, push_notification"""
    ids = [id.strip() for id in entity_ids.split(",")[:50]]
    # Check eligibility for these entities
    id_sql = ",".join(f"'{i}'" for i in ids)
    elig = spark.sql(f"""
        SELECT entity_id, is_eligible, exclusion_reason, contact_address
        FROM {CATALOG}.marketing.marketing_eligibility
        WHERE entity_id IN ({id_sql}) AND channel_type = '{destination_type}'
        AND purpose IN ('RETENTION','CROSS_SELL','UPSELL','WINBACK','RENEWAL_OUTREACH')
    """).collect()
    
    eligible_ids = list(set(r.entity_id for r in elig if r.is_eligible))
    suppressed = [{"entity_id": r.entity_id, "reason": r.exclusion_reason} 
                  for r in elig if not r.is_eligible]
    
    # Simulate delivery outcomes based on historical rates
    hist = spark.sql(f"""
        SELECT delivery_status, COUNT(*) as cnt, ROUND(AVG(match_rate), 3) as avg_match
        FROM {CATALOG}.marketing.activation_log
        WHERE destination_system = '{destination_system}'
        GROUP BY delivery_status
    """).collect()
    
    return json.dumps({
        "simulation": True,
        "activation_id": str(uuid.uuid4()),
        "audience_name": audience_name,
        "destination": {"system": destination_system, "type": destination_type},
        "total_in_segment": len(ids),
        "eligible_for_send": len(eligible_ids),
        "suppressed": len(set(r["entity_id"] for r in suppressed)),
        "suppression_reasons": suppressed[:10],
        "estimated_delivery": {
            "based_on_historical": [r.asDict() for r in hist],
        },
        "eligible_entity_ids": eligible_ids[:20],
        "note": "SIMULATION ONLY — no records written. In production, CustomerLake would write to activation_log and push via connector."
    }, default=str)


def get_cross_source_signals(entity_ids: str) -> str:
    """Get cross-source behavioral signals for audience members that only CustomerLake can provide.
    Combines interaction history, billing signals, and campaign engagement in one view."""
    id_list = entity_ids.replace(" ", "")
    query = f"""
        SELECT p.entity_id, p.name, p.customer_segment, p.lifecycle_status,
               p.churn_risk_score, p.arpu_tier, p.active_service_count,
               p.last_interaction_date, p.outstanding_balance, p.total_billed_amount,
               p.open_problem_count, p.avg_csat_score, p.source_count,
               -- Campaign engagement (from campaign_event via entity_id)
               ce_agg.campaign_touches, ce_agg.last_campaign_touch,
               ce_agg.response_count, ce_agg.meeting_count
        FROM {CATALOG}.gold.customer_profile_360 p
        LEFT JOIN (
            SELECT entity_id, 
                   COUNT(*) as campaign_touches,
                   MAX(event_timestamp) as last_campaign_touch,
                   SUM(CASE WHEN event_type = 'RESPONSE' THEN 1 ELSE 0 END) as response_count,
                   SUM(CASE WHEN event_type = 'MEETING' THEN 1 ELSE 0 END) as meeting_count
            FROM {CATALOG}.marketing.campaign_event
            GROUP BY entity_id
        ) ce_agg ON p.entity_id = ce_agg.entity_id
        WHERE p.entity_id IN ({id_list})
    """
    rows = spark.sql(query).collect()
    return json.dumps([r.asDict() for r in rows], default=str)


# --- Register all tools ---
TOOLS = {
    "build_segment_query": build_segment_query,
    "get_segment_size": get_segment_size,
    "evaluate_eligibility": evaluate_eligibility,
    "check_consent_status": check_consent_status,
    "get_existing_audiences": get_existing_audiences,
    "get_campaign_performance": get_campaign_performance,
    "get_campaign_attribution": get_campaign_attribution,
    "get_activation_delivery_stats": get_activation_delivery_stats,
    "simulate_activation_push": simulate_activation_push,
    "get_cross_source_signals": get_cross_source_signals,
}

TOOL_SCHEMAS = [
    {"type": "function", "function": {"name": "build_segment_query", "description": "Execute a SQL segment query against unified customer profiles. where_clause is the SQL WHERE condition (e.g., \"customer_segment = 'enterprise' AND lifecycle_status = 'active'\"). Returns matching profiles with key attributes.", "parameters": {"type": "object", "properties": {"where_clause": {"type": "string", "description": "SQL WHERE clause for filtering profiles"}, "limit": {"type": "integer", "description": "Max rows to return (default 100)", "default": 100}}, "required": ["where_clause"]}}},
    {"type": "function", "function": {"name": "get_segment_size", "description": "Get count of profiles matching a segment condition plus summary stats (consent rates, lifecycle breakdown, avg churn risk). Use before fetching full audience to validate size.", "parameters": {"type": "object", "properties": {"where_clause": {"type": "string", "description": "SQL WHERE clause"}}, "required": ["where_clause"]}}},
    {"type": "function", "function": {"name": "evaluate_eligibility", "description": "Check marketing eligibility for entity_ids by purpose and channel. Returns eligible/excluded counts with exclusion reasons (NO_OPT_IN, DNC_SUPPRESSED, INVALID_CHANNEL).", "parameters": {"type": "object", "properties": {"entity_ids": {"type": "string", "description": "Comma-separated entity IDs (quote each: '10001','10002')"}, "purpose": {"type": "string", "enum": ["RETENTION", "CROSS_SELL", "UPSELL", "WINBACK", "RENEWAL_OUTREACH"]}, "channel": {"type": "string", "enum": ["email", "telephone", "fax"], "default": "email"}}, "required": ["entity_ids", "purpose"]}}},
    {"type": "function", "function": {"name": "check_consent_status", "description": "Check detailed consent records for entities: opt-in flags, DNC status, validity, verification.", "parameters": {"type": "object", "properties": {"entity_ids": {"type": "string", "description": "Comma-separated entity IDs"}, "channel": {"type": "string", "default": "email"}}, "required": ["entity_ids"]}}},
    {"type": "function", "function": {"name": "get_existing_audiences", "description": "Browse existing audience snapshots with membership counts, dedup stats, and timestamps.", "parameters": {"type": "object", "properties": {"purpose_filter": {"type": "string", "description": "Optional: RETENTION, CROSS_SELL, UPSELL, WINBACK, RENEWAL_OUTREACH"}}, "required": []}}},
    {"type": "function", "function": {"name": "get_campaign_performance", "description": "Get historical campaign performance: sends, opens, clicks, responses, conversions, revenue, and computed rates.", "parameters": {"type": "object", "properties": {"purpose": {"type": "string", "description": "Optional purpose filter"}, "channel": {"type": "string", "description": "Optional channel filter"}}, "required": []}}},
    {"type": "function", "function": {"name": "get_campaign_attribution", "description": "Get campaign attribution showing which touches drove conversions and revenue. Includes avg days-to-convert and attribution weights.", "parameters": {"type": "object", "properties": {"purpose": {"type": "string", "description": "Optional purpose filter"}, "limit": {"type": "integer", "default": 20}}, "required": []}}},
    {"type": "function", "function": {"name": "get_activation_delivery_stats", "description": "Check activation delivery statistics: delivery rates, match rates, conversion outcomes by destination system.", "parameters": {"type": "object", "properties": {"audience_name": {"type": "string", "description": "Optional audience name filter"}, "destination": {"type": "string", "description": "Optional destination system filter"}}, "required": []}}},
    {"type": "function", "function": {"name": "simulate_activation_push", "description": "Simulate pushing an audience segment to a destination (Braze, Salesforce, Google Ads, etc). Returns projected delivery, suppression breakdown, and estimated outcomes. SIMULATION ONLY.", "parameters": {"type": "object", "properties": {"entity_ids": {"type": "string", "description": "Comma-separated entity IDs to activate"}, "destination_system": {"type": "string", "enum": ["Braze", "Salesforce_Campaign", "Google_Ads", "HubSpot", "Twilio", "Meta_Custom_Audience"]}, "destination_type": {"type": "string", "enum": ["email", "sms", "paid_social", "crm_list", "push_notification"]}, "audience_name": {"type": "string", "description": "Name for this activation audience"}}, "required": ["entity_ids", "destination_system", "destination_type", "audience_name"]}}},
    {"type": "function", "function": {"name": "get_cross_source_signals", "description": "Get cross-source behavioral signals for audience members combining profile data, interaction history, and campaign engagement. This is the CustomerLake differentiator — signals no single source system can provide.", "parameters": {"type": "object", "properties": {"entity_ids": {"type": "string", "description": "Comma-separated entity IDs (quote each: '10001','10002')"}}, "required": ["entity_ids"]}}},
]

print(f"Campaign Agent: {len(TOOLS)} tools registered")
for name in TOOLS:
    print(f"  \u2022 {name}")

# COMMAND ----------

# DBTITLE 1,Campaign Agent — core agent loop with tool calling
# ---------------------------------------------------------------------------
# CustomerLake Campaign Agent
# Uses Foundation Model API with tool calling to build audiences from natural
# language, evaluate eligibility/consent, and simulate activation pushes.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = textwrap.dedent("""\
    You are the CustomerLake Campaign Agent. Your job is to help marketers build
    targeted audiences from natural language briefs, evaluate eligibility and consent,
    and simulate activation to downstream marketing systems.

    ## Your capabilities
    You operate on the full CustomerLake data layer in Unity Catalog:
    - 35,671 unified customer profiles with 45 attributes spanning 5 source systems
    - 193,160 eligibility records evaluating consent + channel validity + role status
    - 55,745 audience memberships across 5 purposes and 3 channels
    - 30,000 activation records across 6 destination systems
    - 18,068 campaign events and 3,708 attributed conversions
    - Customer segments: enterprise, medium_business, small_business, government, consumer, wholesale
    - Purposes: RETENTION, CROSS_SELL, UPSELL, WINBACK, RENEWAL_OUTREACH
    - Lifecycle statuses: active, churned, dormant, suspended, lead, prospect, win_back, deceased, merged
    - ARPU tiers: tier_1_high, tier_2_medium, tier_3_low, tier_4_minimal

    ## Your tools
    - build_segment_query: Execute SQL segment against unified profiles (build WHERE clause)
    - get_segment_size: Quick count + stats before full audience pull
    - evaluate_eligibility: Check eligibility per entity × purpose × channel
    - check_consent_status: Detailed consent records (opt-in, DNC, validity)
    - get_existing_audiences: Browse current audience snapshots
    - get_campaign_performance: Historical send/open/click/conversion rates
    - get_campaign_attribution: Which touches drove conversions and revenue
    - get_activation_delivery_stats: Delivery rates by destination system
    - simulate_activation_push: Simulate sending audience to Braze/SF/Google/etc.
    - get_cross_source_signals: Cross-source behavioral signals (the CustomerLake differentiator)

    ## Campaign building protocol
    1. **Understand the brief:** Parse the marketer's intent into segment criteria
    2. **Size the opportunity:** Use get_segment_size to validate before pulling full data
    3. **Build the segment:** Translate criteria to SQL WHERE clause for build_segment_query
    4. **Evaluate eligibility:** Check every audience member against eligibility rules
    5. **Check consent:** Verify opt-in and DNC status per channel
    6. **Enrich with cross-source signals:** Pull behavioral data that only CustomerLake provides
    7. **Review historical performance:** Check what worked before for this purpose/channel
    8. **Simulate activation:** Push to recommended destination with projected outcomes
    9. **Report:** Segment size, eligible audience, channel recommendation, estimated ROI

    ## Output format
    After building the audience, produce a structured campaign report:
    ```
    CAMPAIGN AUDIENCE REPORT
    ========================
    Campaign Brief: [original marketer request]
    
    SEGMENT DEFINITION:
    - SQL criteria: [the WHERE clause used]
    - Total matches: [count]
    - Key characteristics: [segment profile summary]
    
    ELIGIBILITY GATE:
    - Eligible for send: [count] ([%] of segment)
    - Suppressed: [count] with breakdown by reason
    - Net audience size: [final count]
    
    CHANNEL RECOMMENDATION:
    - Recommended channel: [email/telephone/etc] with rationale
    - Historical performance: [open rate, conversion rate for this purpose]
    
    CROSS-SOURCE INTELLIGENCE:
    Insights only possible because CustomerLake unifies across sources:
    - [insight 1: e.g., "42% of segment has billing disputes — adjust messaging tone"]
    - [insight 2: e.g., "avg churn risk is 52K — retention framing will resonate"]
    
    ACTIVATION PLAN:
    - Destination: [Braze/Salesforce/Google/etc]
    - Estimated delivery rate: [X%]
    - Projected conversions: [based on historical attribution]
    - Estimated revenue impact: [$X]
    
    WHAT LEGACY CDPs CAN'T DO HERE:
    - [specific differentiation point 1]
    - [specific differentiation point 2]
    ```

    ## Rules
    - Always validate segment size BEFORE pulling full audience
    - Always check eligibility — never skip consent evaluation
    - Always provide cross-source signals — this is CustomerLake's key differentiator
    - Always reference historical campaign performance for ROI projections
    - Always explain what a legacy CDP could NOT do in this scenario
    - Consent is sacred — suppressed contacts must NEVER be included in activation
    - Show the SQL criteria transparently — marketers should see what the agent built
""")


def run_campaign_agent(user_message: str, verbose: bool = True) -> dict:
    """Run the Campaign Agent with tool calling."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message}
    ]

    if verbose:
        print(f"\n{'='*70}")
        print(f"CAMPAIGN AGENT \u2014 Audience build started")
        print(f"{'='*70}")
        print(f"Brief: {user_message[:300]}")

    tool_calls_made = []
    for turn in range(MAX_TURNS):
        response = client.chat.completions.create(
            model=MODEL_ENDPOINT,
            messages=messages,
            tools=TOOL_SCHEMAS,
            max_tokens=4096,
        )
        choice = response.choices[0]

        if choice.finish_reason == "tool_calls" and choice.message.tool_calls:
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)
                if verbose:
                    args_str = json.dumps(fn_args, default=str)
                    if len(args_str) > 200:
                        args_str = args_str[:200] + "..."
                    print(f"\n  \u21B3 Tool call [{turn+1}]: {fn_name}({args_str})")
                tool_calls_made.append({"tool": fn_name, "args": fn_args})
                try:
                    result = TOOLS[fn_name](**fn_args)
                    if len(result) > 8000:
                        result = result[:8000] + '\n... (truncated)'
                except Exception as e:
                    result = json.dumps({"error": str(e)})
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result
                })
        else:
            final_text = choice.message.content
            if verbose:
                print(f"\n{'='*70}")
                print("CAMPAIGN AUDIENCE REPORT")
                print(f"{'='*70}")
                print(final_text)
            return {
                "report": final_text,
                "turns": turn + 1,
                "tool_calls": tool_calls_made,
                "messages": messages
            }

    return {"report": "MAX_TURNS reached", "turns": MAX_TURNS, "tool_calls": tool_calls_made, "messages": messages}


print("Campaign Agent ready.")

# COMMAND ----------

# DBTITLE 1,Scene 1 — Enterprise Retention Campaign from Natural Language
# MAGIC %md
# MAGIC ## Scene 1: Enterprise Retention Campaign from Natural Language
# MAGIC
# MAGIC The marketing director gives a natural language brief:
# MAGIC > "I need to target our enterprise customers who are at high risk of churning. They should be active accounts with significant billing history. We want to reach them via email through Braze with a retention offer before they leave."
# MAGIC
# MAGIC The Campaign Agent must:
# MAGIC 1. Translate the brief into SQL segment criteria against `customer_profile_360`
# MAGIC 2. Size the opportunity before pulling the full audience
# MAGIC 3. Evaluate eligibility and consent for email channel
# MAGIC 4. Pull cross-source signals that reveal WHY they're at risk (billing disputes? CSAT decline? service problems?)
# MAGIC 5. Check historical retention campaign performance
# MAGIC 6. Simulate activation push to Braze
# MAGIC
# MAGIC **CustomerLake differentiation:**
# MAGIC - A legacy CDP would let you filter by segment = enterprise and a risk score threshold — but it can't tell you *why* they're at risk or combine billing + service + interaction signals.
# MAGIC - CustomerLake's Campaign Agent reasons across sources to produce actionable intelligence for the retention team.

# COMMAND ----------

# DBTITLE 1,Run Scene 1 — Enterprise retention campaign
scene_1 = run_campaign_agent(
    """I need to build a retention campaign targeting our enterprise customers who are at high 
    risk of churning. Here's what I'm looking for:
    
    - Segment: Enterprise customers only
    - Status: Active accounts (not already churned)
    - Risk: High churn risk (above average for their segment)
    - Value: Prefer high-ARPU accounts where churn would hurt revenue
    - Channel: Email via Braze
    - Purpose: RETENTION
    
    I want to understand:
    1. How big is this segment?
    2. How many can we actually reach (after consent/eligibility)?
    3. What cross-source signals tell us about WHY they're at risk?
    4. What has our historical retention email performance looked like?
    5. What's the projected delivery and conversion if we push to Braze?
    
    Build me the audience and simulate the activation."""
)

# COMMAND ----------

# DBTITLE 1,Scene 2 — Win-Back Campaign with ROI Measurement
# MAGIC %md
# MAGIC ## Scene 2: Win-Back Campaign with Closed-Loop ROI Measurement
# MAGIC
# MAGIC The CMO wants to win back churned customers who had high lifetime value. But she's skeptical:
# MAGIC > "Every CDP promises win-back campaigns. Show me what CustomerLake does differently. I want to see the full loop: who to target, why they left, what channel works best, and what the ROI projection looks like based on actual attribution data — not just vanity metrics."
# MAGIC
# MAGIC **CustomerLake differentiation:**
# MAGIC - Legacy CDPs can filter `status = churned` and send a blast. They can't explain WHY each customer churned.
# MAGIC - CustomerLake combines billing disputes, CSAT scores, service problems, and interaction history to segment churned customers by churn driver — enabling personalized win-back messaging.
# MAGIC - The agent closes the loop with attribution data: actual conversion amounts and days-to-convert from past win-back campaigns.

# COMMAND ----------

# DBTITLE 1,Run Scene 2 — Win-back campaign with ROI
scene_2 = run_campaign_agent(
    """The CMO is challenging our win-back strategy. She wants a sophisticated win-back 
    campaign that proves CustomerLake's value over legacy CDPs.
    
    Campaign brief:
    - Target: Customers who have churned but were previously high-value (tier_1_high or tier_2_medium ARPU)
    - Must have had multiple source systems contributing data (source_count > 2) — these are customers 
      we know well enough to win back
    - Channel: Email (purpose: WINBACK)
    
    The CMO specifically wants to see:
    1. Segment size and characteristics
    2. Cross-source signals explaining WHY they churned (billing issues? service problems? low engagement?)
    3. Historical win-back campaign performance with ACTUAL attribution data (not projections)
    4. Eligibility after consent gating — how many can we actually reach?
    5. Which destination system has the best delivery rate for win-back?
    6. A concrete ROI projection backed by historical attribution
    
    She'll reject anything a Braze or Iterable could do on their own. Show the cross-source 
    intelligence that makes this campaign smarter."""
)

# COMMAND ----------

# DBTITLE 1,Scene 3 — AI-Discovered Cross-Sell Audience
# MAGIC %md
# MAGIC ## Scene 3: AI-Discovered Cross-Sell Audience (the "Aha" Moment)
# MAGIC
# MAGIC This scene demonstrates what no legacy CDP can do: the Campaign Agent proactively discovers a cross-sell opportunity by reasoning across billing, service, and interaction signals.
# MAGIC
# MAGIC > "Don't just build what I ask for. Look at our data and TELL ME what campaign I should be running that I haven't thought of. Find an audience I'm missing."
# MAGIC
# MAGIC **CustomerLake differentiation:**
# MAGIC - Legacy CDPs are reactive: they build what you ask for. They can't discover opportunities.
# MAGIC - CustomerLake's Campaign Agent can reason across unified profiles to identify non-obvious audience segments — customers with expiring contracts + rising ARPU + no recent campaign touches = untapped cross-sell gold.
# MAGIC - This is the demo scene that makes CMOs sit up: the platform is smarter than the marketer's intuition.

# COMMAND ----------

# DBTITLE 1,Run Scene 3 — AI-discovered cross-sell audience
scene_3 = run_campaign_agent(
    """I want you to be proactive. Don't wait for me to tell you what audience to build.
    
    Analyze our CustomerLake data and DISCOVER a high-value campaign opportunity that 
    we're currently missing. Look for:
    
    1. Customers we have rich data on (high source_count, high resolution_confidence) but 
       haven't been targeting in campaigns
    2. Cross-source signals that suggest untapped revenue potential
    3. Segments where our existing audiences (RETENTION, CROSS_SELL, UPSELL, WINBACK, 
       RENEWAL_OUTREACH) have gaps
    
    Start by checking our existing audiences to understand what we're already doing,
    then analyze the profile data to find what we're missing. Build the audience,
    check eligibility, and recommend the best activation channel.
    
    The CMO will ask: "What does CustomerLake see that our current tools don't?"
    Make sure the answer is compelling."""
)

# COMMAND ----------

# DBTITLE 1,Differentiation Summary
# MAGIC %md
# MAGIC ## CustomerLake Campaign Agent — Differentiation Summary
# MAGIC
# MAGIC | Capability | Legacy CDP (Braze/Iterable/Salesforce) | CustomerLake Campaign Agent |
# MAGIC |---|---|---|
# MAGIC | **Audience building** | Point-and-click segment builder with pre-defined attributes | Natural language → SQL segment with AI-generated criteria |
# MAGIC | **Data sources** | Only data synced into the CDP (typically CRM + email) | Queries across ALL lakehouse sources via identity graph (billing, ERP, CRM, service, behavioral) |
# MAGIC | **Eligibility** | Simple opt-in check at send time | Multi-factor: consent × channel validity × DNC suppression × jurisdiction × role status |
# MAGIC | **Exclusion transparency** | "Contact suppressed" (no reason) | Detailed reason codes: NO_OPT_IN, DNC_SUPPRESSED, INVALID_CHANNEL |
# MAGIC | **Cross-source intelligence** | Impossible — data lives in one system | Combines billing disputes + CSAT + service problems + campaign engagement |
# MAGIC | **Campaign discovery** | Marketer must define every audience manually | Agent proactively identifies untapped segments from cross-source patterns |
# MAGIC | **ROI measurement** | Click/open vanity metrics | Full attribution: conversion amounts, days-to-convert, channel comparison |
# MAGIC | **Closed loop** | Send → open → click (no revenue attribution) | Send → deliver → convert → attributed revenue → ROI per channel |
# MAGIC | **Activation** | Native sends only | Simulate/push to ANY downstream system (Braze, SF, Google Ads, Meta, HubSpot, Twilio) |
# MAGIC | **Scalability** | Limited by CDP's data model | Any Unity Catalog table becomes a segment criterion |

# COMMAND ----------

# DBTITLE 1,Summary statistics — agent performance across scenes
# Summarize agent performance across all scenes
scenes = {
    "Scene 1: Enterprise Retention": scene_1,
    "Scene 2: Win-Back with ROI": scene_2,
    "Scene 3: AI-Discovered Cross-Sell": scene_3,
}

print("\n" + "="*70)
print("CAMPAIGN AGENT \u2014 PERFORMANCE SUMMARY")
print("="*70)
print(f"{'Scene':<42s} {'Turns':>6s} {'Tool Calls':>11s}")
print("-"*70)
for name, result in scenes.items():
    n_tools = len(result.get('tool_calls', []))
    turns = result.get('turns', 0)
    print(f"{name:<42s} {turns:>6d} {n_tools:>11d}")

# Tool usage breakdown
print(f"\n{'Tool Usage Breakdown':}")
print("-"*70)
all_tools = {}
for result in scenes.values():
    for tc in result.get('tool_calls', []):
        tool = tc['tool']
        all_tools[tool] = all_tools.get(tool, 0) + 1
for tool, count in sorted(all_tools.items(), key=lambda x: -x[1]):
    print(f"  {tool:<45s} {count:>3d} calls")

print(f"\nTotal tool calls: {sum(all_tools.values())}")
print(f"Average turns per scene: {sum(r.get('turns',0) for r in scenes.values()) / len(scenes):.1f}")
print(f"Tools utilized: {len(all_tools)}/{len(TOOLS)}")
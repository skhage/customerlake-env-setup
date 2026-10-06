# Databricks notebook source
# DBTITLE 1,CustomerLake Profile Agent
# MAGIC %md
# MAGIC # CustomerLake Profile Agent
# MAGIC
# MAGIC **Purpose:** Demonstrate CustomerLake's Agentic Profile Enrichment — the ability to take a partial or stale customer record, investigate across every source system in the lakehouse, resolve identity, fill gaps with cross-source signals, and produce an enriched golden record with full provenance.
# MAGIC
# MAGIC **What legacy CDPs can't do:**
# MAGIC - Legacy CDPs run scheduled batch ETL merges with static priority rules — they can't investigate on demand.
# MAGIC - Legacy CDPs produce a merged record but don't explain *why* one value was chosen over another.
# MAGIC - CustomerLake's Profile Agent **reasons over conflicting attributes** from TMF, Salesforce, Oracle ERP, and D&B — weighing recency, authority, and completeness — then produces an enriched profile with confidence scores and source attribution.
# MAGIC - The agent accesses the full 360 profile (35.7K entities, 5 source systems, 254K relationships) and can traverse the identity graph to find enrichment signals no single source contains.
# MAGIC
# MAGIC **Data scope:**
# MAGIC - `gold.customer_profile_360` — 35.7K unified profiles (45 attributes per entity)
# MAGIC - `identity.entity_xref` — 60.7K source-to-entity crossreferences (Salesforce, TMF, Oracle ERP, D&B, Marketing)
# MAGIC - `identity.attribute_survivorship` — source value selection with reasoning
# MAGIC - `salesforce_source.*` — CRM contacts, accounts, contracts, opportunities
# MAGIC - `oracle_erp_source.*` — ERP customer accounts, AR, billing rates
# MAGIC - `gold.typed_relationship` — 254K relationship edges
# MAGIC - `tmf_customer.interaction` — 100K customer interactions
# MAGIC
# MAGIC **Demo scenes:**
# MAGIC 1. **New CRM Contact Enrichment** — A Salesforce contact arrives with partial data. Agent resolves identity, pulls cross-source attributes, builds complete golden record.
# MAGIC 2. **Churn Risk Investigation** — A high-risk customer is flagged. Agent assembles full profile, checks interactions, billing disputes, service issues, and produces retention-focused enrichment.
# MAGIC 3. **Profile Completeness Audit** — Agent analyzes profiles with data gaps, identifies which sources could fill them, and produces a prioritized enrichment plan.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openai -q

# COMMAND ----------

# DBTITLE 1,Setup — imports and configuration
import json, textwrap
from datetime import datetime
from openai import OpenAI
from pyspark.sql import functions as F

# --- Configuration ---
CATALOG = "cdm_tmforum"
MODEL_ENDPOINT = "databricks-claude-sonnet-4-5"  # Best reasoning for cross-source evidence weighing
MAX_TURNS = 10  # Agent reasoning loop limit

# Foundation Model API client (OpenAI-compatible)
client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url=f"https://{spark.conf.get('spark.databricks.workspaceUrl').rstrip('/')}/serving-endpoints"
)

print(f"Profile Agent configured")
print(f"  Model: {MODEL_ENDPOINT}")
print(f"  Catalog: {CATALOG}")
print(f"  Profile view: {CATALOG}.gold.customer_profile_360 (35.7K entities, 45 attributes)")
print(f"  Source systems: TMF_PARTY, SALESFORCE, ORACLE_ERP, MOCK_DNB, MOCK_MARKETING")

# COMMAND ----------

# DBTITLE 1,Tool definitions — SQL functions the agent calls
# ---------------------------------------------------------------------------
# Profile Agent tools
# Each tool runs scoped SQL queries across the CustomerLake data layer and
# returns structured JSON the agent can reason over.
# ---------------------------------------------------------------------------

def get_customer_profile(entity_id: str) -> str:
    """Get the full 360 profile for an entity from the golden record."""
    df = spark.sql(f"""
        SELECT * FROM {CATALOG}.gold.customer_profile_360
        WHERE entity_id = '{entity_id}'
    """)
    rows = df.collect()
    if not rows:
        return json.dumps({"error": f"No profile found for entity_id {entity_id}"})
    profile = rows[0].asDict()
    return json.dumps(profile, default=str)


def search_customers(search_term: str, search_field: str = "email") -> str:
    """Search for customer profiles by email, phone, or name fragment.
    search_field: 'email', 'phone', 'name', or 'segment'"""
    field_map = {
        "email": "primary_email",
        "phone": "primary_phone",
        "name": "name",
        "segment": "customer_segment"
    }
    col = field_map.get(search_field, "primary_email")
    if search_field == "segment":
        where = f"{col} = '{search_term}'"
    else:
        where = f"LOWER({col}) LIKE LOWER('%{search_term}%')"
    df = spark.sql(f"""
        SELECT entity_id, entity_type, name, customer_segment, primary_email, primary_phone,
               lifecycle_status, churn_risk_score, arpu_tier, source_count, xref_count,
               resolution_confidence, active_service_count, consent_marketing
        FROM {CATALOG}.gold.customer_profile_360
        WHERE {where}
        LIMIT 10
    """)
    return json.dumps([r.asDict() for r in df.collect()], default=str)


def get_source_records(entity_id: str) -> str:
    """Get all source system records linked to this entity via identity crosswalk.
    Shows which systems contribute data and what each record contains."""
    xrefs = spark.sql(f"""
        SELECT xref_id, source_instance, object_type, source_record_id,
               match_rule, confidence, is_current, valid_from
        FROM {CATALOG}.identity.entity_xref
        WHERE entity_id = '{entity_id}' AND is_current = true
        ORDER BY source_instance
    """).collect()
    return json.dumps({
        "entity_id": entity_id,
        "source_count": len(set(r.source_instance for r in xrefs)),
        "crossreferences": [r.asDict() for r in xrefs]
    }, default=str)


def get_salesforce_details(entity_id: str) -> str:
    """Enrich from Salesforce: contact demographics, account details, contracts, opportunities."""
    # Get SF contact via crosswalk
    contacts = spark.sql(f"""
        SELECT c.Id, c.FirstName, c.LastName, c.Email, c.Phone, c.Title,
               c.Department, c.MailingCity, c.MailingState, c.MailingCountry, c.IsActive,
               a.Name as account_name, a.Industry, a.AnnualRevenue, a.NumberOfEmployees,
               a.BillingCountry, a.Type as account_type
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.salesforce_source.contact c ON x.source_record_id = c.Id
        LEFT JOIN {CATALOG}.salesforce_source.account a ON c.AccountId = a.Id
        WHERE x.entity_id = '{entity_id}' AND x.source_instance = 'SALESFORCE' 
              AND x.object_type = 'contact' AND x.is_current = true
    """).collect()
    
    # Get SF contracts
    contracts = spark.sql(f"""
        SELECT ct.Id, ct.Status, ct.StartDate, ct.EndDate, ct.ContractTerm,
               ct.BillingCity, ct.BillingState
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.salesforce_source.contact c ON x.source_record_id = c.Id
        JOIN {CATALOG}.salesforce_source.contract ct ON c.AccountId = ct.AccountId
        WHERE x.entity_id = '{entity_id}' AND x.source_instance = 'SALESFORCE'
              AND x.object_type = 'contact' AND x.is_current = true
        LIMIT 5
    """).collect()
    
    # Get SF opportunities
    opps = spark.sql(f"""
        SELECT o.Id, o.Name, o.StageName, o.Amount, o.CloseDate, o.Type as opp_type
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.salesforce_source.contact c ON x.source_record_id = c.Id
        JOIN {CATALOG}.salesforce_source.opportunity o ON c.AccountId = o.AccountId
        WHERE x.entity_id = '{entity_id}' AND x.source_instance = 'SALESFORCE'
              AND x.object_type = 'contact' AND x.is_current = true
        ORDER BY o.CloseDate DESC
        LIMIT 5
    """).collect()
    
    return json.dumps({
        "contacts": [r.asDict() for r in contacts],
        "contracts": [r.asDict() for r in contracts],
        "opportunities": [r.asDict() for r in opps]
    }, default=str)


def get_oracle_erp_details(entity_id: str) -> str:
    """Enrich from Oracle ERP: customer accounts, AR balances, billing rates."""
    accounts = spark.sql(f"""
        SELECT h.CUST_ACCOUNT_ID, h.PARTY_NAME, h.PARTY_TYPE, h.ACCOUNT_NUMBER,
               h.STATUS, h.ORIG_SYSTEM_REFERENCE
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.oracle_erp_source.hz_cust_accounts h 
             ON x.source_record_id = CAST(h.CUST_ACCOUNT_ID AS STRING)
        WHERE x.entity_id = '{entity_id}' AND x.source_instance = 'ORACLE_ERP'
              AND x.is_current = true
    """).collect()
    
    # Get AR transactions for revenue context
    ar_trx = spark.sql(f"""
        SELECT t.TRX_NUMBER, t.TRX_DATE, t.INVOICE_CURRENCY_CODE,
               l.EXTENDED_AMOUNT, l.LINE_TYPE
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.oracle_erp_source.hz_cust_accounts h 
             ON x.source_record_id = CAST(h.CUST_ACCOUNT_ID AS STRING)
        JOIN {CATALOG}.oracle_erp_source.ra_customer_trx_all t 
             ON h.CUST_ACCOUNT_ID = t.BILL_TO_CUSTOMER_ID
        JOIN {CATALOG}.oracle_erp_source.ra_customer_trx_lines_all l 
             ON t.CUSTOMER_TRX_ID = l.CUSTOMER_TRX_ID
        WHERE x.entity_id = '{entity_id}' AND x.source_instance = 'ORACLE_ERP'
              AND x.is_current = true
        ORDER BY t.TRX_DATE DESC
        LIMIT 10
    """).collect()
    
    return json.dumps({
        "erp_accounts": [r.asDict() for r in accounts],
        "ar_transactions": [r.asDict() for r in ar_trx]
    }, default=str)


def get_survivorship_history(entity_id: str) -> str:
    """Get attribute survivorship decisions showing which source value won for each attribute and why."""
    # Get entity version ID first
    versions = spark.sql(f"""
        SELECT entity_version_id FROM {CATALOG}.identity.entity_version
        WHERE entity_id = '{entity_id}'
        ORDER BY version_timestamp DESC
        LIMIT 1
    """).collect()
    if not versions:
        return json.dumps({"error": "No version history found"})
    
    ver_id = versions[0].entity_version_id
    surv = spark.sql(f"""
        SELECT attribute_name, selected_source, selected_record_id,
               selected_value, source_authority, rejected_alternatives,
               policy_version, decided_at
        FROM {CATALOG}.identity.attribute_survivorship
        WHERE entity_version_id = '{ver_id}'
        ORDER BY attribute_name
    """).collect()
    
    return json.dumps({
        "entity_id": entity_id,
        "version_id": ver_id,
        "survivorship_decisions": [r.asDict() for r in surv]
    }, default=str)


def get_interaction_history(entity_id: str, limit: int = 10) -> str:
    """Get recent customer interactions for behavioral context."""
    # Map entity to customer_id via crosswalk
    interactions = spark.sql(f"""
        SELECT i.interaction_id, i.type as interaction_type, i.channel, i.category,
               i.start_timestamp, i.state as resolution_status, 
               i.customer_satisfaction_score as csat_score,
               i.escalation_flag, i.first_contact_resolution_flag,
               i.subject, i.priority
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.tmf_customer.interaction i 
             ON x.source_record_id = CAST(i.customer_id AS STRING)
        WHERE x.entity_id = '{entity_id}' 
              AND x.source_instance = 'TMF_PARTY'
              AND x.is_current = true
        ORDER BY i.start_timestamp DESC
        LIMIT {limit}
    """).collect()
    return json.dumps([r.asDict() for r in interactions], default=str)


def get_billing_summary(entity_id: str) -> str:
    """Get billing and payment summary for financial context."""
    billing = spark.sql(f"""
        SELECT b.bill_id, b.date as billing_date, b.total_amount, b.outstanding_amount,
               b.state as bill_state, b.payment_method, b.currency_code
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.tmf_customer.bill b 
             ON x.source_record_id = CAST(b.customer_id AS STRING)
        WHERE x.entity_id = '{entity_id}' 
              AND x.source_instance = 'TMF_PARTY'
              AND x.is_current = true
        ORDER BY b.date DESC
        LIMIT 10
    """).collect()
    
    payments = spark.sql(f"""
        SELECT p.payment_id, p.date as payment_date, p.amount as total_amount, 
               p.state as payment_status, p.method_type, p.channel
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.tmf_customer.payment p 
             ON x.source_record_id = CAST(p.customer_id AS STRING)
        WHERE x.entity_id = '{entity_id}' 
              AND x.source_instance = 'TMF_PARTY'
              AND x.is_current = true
        ORDER BY p.date DESC
        LIMIT 10
    """).collect()
    
    disputes = spark.sql(f"""
        SELECT d.billing_dispute_id, d.dispute_type, d.dispute_state, d.disputed_amount,
               d.raised_date, d.actual_resolution_date, d.reason_description,
               d.priority, d.escalation_flag
        FROM {CATALOG}.identity.entity_xref x
        JOIN {CATALOG}.tmf_customer.billing_dispute d 
             ON x.source_record_id = CAST(d.customer_id AS STRING)
        WHERE x.entity_id = '{entity_id}' 
              AND x.source_instance = 'TMF_PARTY'
              AND x.is_current = true
        ORDER BY d.raised_date DESC
        LIMIT 5
    """).collect()
    
    return json.dumps({
        "bills": [r.asDict() for r in billing],
        "payments": [r.asDict() for r in payments],
        "disputes": [r.asDict() for r in disputes]
    }, default=str)


def get_entity_relationships(entity_id: str) -> str:
    """Get typed relationships from the gold layer for org hierarchy and business context."""
    df = spark.sql(f"""
        SELECT relationship_type, from_entity_id, to_entity_id,
               from_role, to_role, source_instance, is_current
        FROM {CATALOG}.gold.typed_relationship
        WHERE (from_entity_id = '{entity_id}' OR to_entity_id = '{entity_id}')
          AND is_current = true
        LIMIT 20
    """)
    return json.dumps([r.asDict() for r in df.collect()], default=str)


def get_profile_completeness_batch(segment: str = None, limit: int = 10) -> str:
    """Analyze profile completeness for a batch of entities. Identifies gaps."""
    where = f"WHERE customer_segment = '{segment}'" if segment else ""
    df = spark.sql(f"""
        SELECT entity_id, entity_type, name, customer_segment, source_count,
            CASE WHEN primary_email IS NULL THEN 0 ELSE 1 END +
            CASE WHEN primary_phone IS NULL THEN 0 ELSE 1 END +
            CASE WHEN mobile_phone IS NULL THEN 0 ELSE 1 END +
            CASE WHEN country_of_residence IS NULL THEN 0 ELSE 1 END +
            CASE WHEN preferred_language IS NULL THEN 0 ELSE 1 END +
            CASE WHEN churn_risk_score IS NULL THEN 0 ELSE 1 END +
            CASE WHEN arpu_tier IS NULL THEN 0 ELSE 1 END +
            CASE WHEN last_interaction_date IS NULL THEN 0 ELSE 1 END +
            CASE WHEN total_billed_amount IS NULL OR total_billed_amount = 0 THEN 0 ELSE 1 END +
            CASE WHEN consent_marketing IS NULL THEN 0 ELSE 1 END
            AS filled_fields,
            10 AS total_key_fields,
            ARRAY_COMPACT(ARRAY(
                CASE WHEN primary_email IS NULL THEN 'primary_email' END,
                CASE WHEN primary_phone IS NULL THEN 'primary_phone' END,
                CASE WHEN mobile_phone IS NULL THEN 'mobile_phone' END,
                CASE WHEN country_of_residence IS NULL THEN 'country_of_residence' END,
                CASE WHEN preferred_language IS NULL THEN 'preferred_language' END,
                CASE WHEN churn_risk_score IS NULL THEN 'churn_risk_score' END,
                CASE WHEN arpu_tier IS NULL THEN 'arpu_tier' END,
                CASE WHEN last_interaction_date IS NULL THEN 'last_interaction_date' END,
                CASE WHEN total_billed_amount IS NULL OR total_billed_amount = 0 THEN 'billing_data' END,
                CASE WHEN consent_marketing IS NULL THEN 'consent_marketing' END
            )) AS missing_fields
        FROM {CATALOG}.gold.customer_profile_360
        {where}
        ORDER BY filled_fields ASC, source_count ASC
        LIMIT {limit}
    """)
    return json.dumps([r.asDict() for r in df.collect()], default=str)


# Tool registry
TOOLS = {
    "get_customer_profile": get_customer_profile,
    "search_customers": search_customers,
    "get_source_records": get_source_records,
    "get_salesforce_details": get_salesforce_details,
    "get_oracle_erp_details": get_oracle_erp_details,
    "get_survivorship_history": get_survivorship_history,
    "get_interaction_history": get_interaction_history,
    "get_billing_summary": get_billing_summary,
    "get_entity_relationships": get_entity_relationships,
    "get_profile_completeness_batch": get_profile_completeness_batch,
}

# OpenAI function-calling schemas
TOOL_SCHEMAS = [
    {"type": "function", "function": {
        "name": "get_customer_profile",
        "description": "Get the full 360 golden record profile for a customer entity. Returns all 45 attributes including demographics, lifecycle, identity resolution metadata, installed base, interactions, billing, and consent.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to look up"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "search_customers",
        "description": "Search for customer profiles by email, phone, name fragment, or segment. Returns matching profiles with key attributes.",
        "parameters": {"type": "object", "properties": {
            "search_term": {"type": "string", "description": "The value to search for"},
            "search_field": {"type": "string", "enum": ["email", "phone", "name", "segment"], "description": "Which field to search (default: email)"}
        }, "required": ["search_term"]}
    }},
    {"type": "function", "function": {
        "name": "get_source_records",
        "description": "Get all source system records linked to this entity via the identity crosswalk. Shows which systems (Salesforce, TMF, Oracle ERP, D&B, Marketing) contribute data and the confidence of each link.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to look up"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_salesforce_details",
        "description": "Enrich from Salesforce CRM: contact demographics, account industry/revenue, active contracts, and pipeline opportunities. Use this to get CRM-specific attributes not in the golden record.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to enrich"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_oracle_erp_details",
        "description": "Enrich from Oracle ERP: customer accounts, AR transaction history, billing rates. Use this to get financial data not in the golden record.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to enrich"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_survivorship_history",
        "description": "Get attribute survivorship decisions showing which source value won for each attribute and why. Reveals the reasoning behind the golden record values.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to examine"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_interaction_history",
        "description": "Get recent customer interactions (support calls, service requests, complaints) for behavioral context and sentiment signals.",
        "parameters": {"type": "object", "properties": {
            "entity_id": {"type": "string", "description": "The entity_id to look up"},
            "limit": {"type": "integer", "description": "Max interactions to return (default 10)"}
        }, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_billing_summary",
        "description": "Get billing invoices, payment history, and any active disputes for financial context.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to look up"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_entity_relationships",
        "description": "Get typed relationships from the gold layer: parent-subsidiary, account roles, service associations. Use for org hierarchy context.",
        "parameters": {"type": "object", "properties": {"entity_id": {"type": "string", "description": "The entity_id to look up"}}, "required": ["entity_id"]}
    }},
    {"type": "function", "function": {
        "name": "get_profile_completeness_batch",
        "description": "Analyze a batch of profiles for data completeness gaps. Returns profiles with the most missing fields, ranked by incompleteness.",
        "parameters": {"type": "object", "properties": {
            "segment": {"type": "string", "description": "Optional customer segment filter (enterprise, medium_business, small_business, consumer)"},
            "limit": {"type": "integer", "description": "Max profiles to return (default 10)"}
        }}
    }},
]

print(f"Profile Agent: {len(TOOLS)} tools registered")
for name in TOOLS:
    print(f"  \u2022 {name}")

# COMMAND ----------

# DBTITLE 1,Profile Agent — core agent loop with tool calling
# ---------------------------------------------------------------------------
# CustomerLake Profile Agent
# Uses Foundation Model API with tool calling to investigate customer profiles,
# enrich from cross-source signals, and produce actionable golden records.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = textwrap.dedent("""\
    You are the CustomerLake Profile Agent. Your job is to investigate customer
    profiles, enrich them from every available source system, identify data gaps,
    and produce comprehensive golden records with full source attribution.

    ## Your capabilities
    You have access to the full CustomerLake data layer in Unity Catalog:
    - 35,671 unified customer profiles (organizations and individuals)
    - 5 source systems: TMF Party (billing/operations), Salesforce (CRM), Oracle ERP (finance), D&B (firmographic), Marketing (campaigns)
    - 60,700 source-to-entity crossreferences with confidence scores
    - 254,209 typed relationships (parent-subsidiary, account roles, service associations)
    - 100,000 customer interactions with CSAT and resolution tracking
    - Full attribute survivorship history showing which source value won and why

    ## Your tools
    - get_customer_profile: Full 360 golden record (45 attributes)
    - search_customers: Find entities by email, phone, name, or segment
    - get_source_records: All source system links via identity crosswalk
    - get_salesforce_details: CRM contacts, accounts, contracts, opportunities
    - get_oracle_erp_details: ERP accounts, AR transactions, billing rates
    - get_survivorship_history: Which source value won for each attribute and why
    - get_interaction_history: Recent support/service interactions
    - get_billing_summary: Invoices, payments, disputes
    - get_entity_relationships: Org hierarchy and business relationships
    - get_profile_completeness_batch: Batch gap analysis across profiles

    ## Investigation protocol
    1. Start with the golden record (get_customer_profile) to see current state
    2. Check source records to understand which systems contribute data
    3. Pull source-specific details (Salesforce, Oracle ERP) for enrichment
    4. Review survivorship history to understand value selection reasoning
    5. Check interactions and billing for behavioral/financial signals
    6. Examine relationships for org hierarchy context

    ## Output format
    After investigation, produce a structured enrichment report:
    ```
    PROFILE ENRICHMENT REPORT
    ========================
    Entity: [entity_id] | Type: [org/individual] | Name: [display name]
    
    CURRENT STATE:
    - Completeness: [X/45 fields populated]
    - Source systems: [list with contribution counts]
    - Resolution confidence: [score]
    
    ENRICHMENT FINDINGS:
    For each attribute gap or conflict found:
    - Attribute: [name]
    - Current value: [from golden record]
    - Alternative values: [from other sources, with source attribution]
    - Recommended value: [which to use and why]
    - Confidence: [0-1]
    
    CROSS-SOURCE SIGNALS:
    Insights only possible by combining data across sources:
    - [signal 1: e.g., "CRM shows active opportunity but billing shows payment disputes"]
    - [signal 2: e.g., "Oracle ERP revenue $X but Salesforce ACV is $Y — discrepancy"]
    
    RISK INDICATORS:
    - Churn risk: [assessment with supporting evidence]
    - Revenue risk: [billing/payment signals]
    - Relationship risk: [interaction sentiment, escalations]
    
    RECOMMENDED ACTIONS:
    1. [specific enrichment or data quality action]
    2. [business action based on cross-source insights]
    ```

    ## Rules
    - Always show source attribution — which system provided each value
    - Highlight cross-source signals that no single system could provide alone
    - Flag data conflicts between sources with recommendations
    - Consent flags are sacred — never recommend overriding consent
    - When data is missing, identify which source system COULD fill the gap
    - Quantify business impact where possible (revenue at risk, ARPU tier, etc.)
""")


def run_profile_agent(user_message: str, verbose: bool = True) -> dict:
    """Run the Profile Agent with tool calling."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message}
    ]

    if verbose:
        print(f"\n{'='*70}")
        print(f"PROFILE AGENT — Investigation started")
        print(f"{'='*70}")
        print(f"Query: {user_message[:200]}")

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
                    print(f"\n  \u21B3 Tool call [{turn+1}]: {fn_name}({fn_args})")
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
                print("PROFILE ENRICHMENT REPORT")
                print(f"{'='*70}")
                print(final_text)
            return {
                "report": final_text,
                "turns": turn + 1,
                "tool_calls": tool_calls_made,
                "messages": messages
            }

    return {"report": "MAX_TURNS reached", "turns": MAX_TURNS, "tool_calls": tool_calls_made, "messages": messages}


print("Profile Agent ready.")

# COMMAND ----------

# DBTITLE 1,Scene 1 — New CRM Contact Enrichment
# MAGIC %md
# MAGIC ## Scene 1: New CRM Contact Enrichment
# MAGIC
# MAGIC A new Salesforce contact has just been synced. The CRM record has basic demographics (name, email, title) but is missing phone, billing history, service details, consent status, and churn risk.
# MAGIC
# MAGIC The Profile Agent must:
# MAGIC 1. Find the entity in CustomerLake via email lookup
# MAGIC 2. Discover all linked source systems (TMF, Oracle ERP, D&B)
# MAGIC 3. Pull enrichment from each source
# MAGIC 4. Show which source contributed each attribute
# MAGIC 5. Identify remaining gaps and recommend actions
# MAGIC
# MAGIC **Why this matters for CustomerLake:**
# MAGIC - A legacy CDP would wait for the next nightly batch merge. CustomerLake enriches on demand.
# MAGIC - A legacy CDP merges by static priority ("always prefer CRM"). CustomerLake weighs recency, authority, and completeness.
# MAGIC - Only CustomerLake can show the full provenance chain: "phone came from TMF Party, revenue from Oracle ERP, firmographic from D&B."

# COMMAND ----------

# DBTITLE 1,Run Scene 1 — New Salesforce contact enrichment
# Scene 1: A new Salesforce contact arrives with partial data.
# Pick a real entity with multiple source systems for a rich demo.
scene_1 = run_profile_agent(
    """A new Salesforce contact has been synced to CustomerLake:
    
    - Email: brandirich@example.org
    - Source: Salesforce CRM
    
    This contact has basic CRM data but the sales team needs a complete profile
    before their next call. Your job:
    
    1. Search for this email in CustomerLake to find the unified entity
    2. Pull the full golden record to see current state
    3. Check which source systems are linked and what each contributes
    4. Enrich from Salesforce (contracts, opportunities), Oracle ERP (financials), 
       and any other available sources
    5. Review survivorship history to show which source won for each attribute
    6. Check recent interactions and billing for behavioral signals
    
    Produce a complete enrichment report showing:
    - What the golden record looks like today
    - What each source system contributed
    - Cross-source signals only possible with unified data
    - Any data conflicts between sources
    - Remaining gaps and which systems could fill them
    - Business-relevant insights for the sales team
    """)

# COMMAND ----------

# DBTITLE 1,Scene 2 — High Churn Risk Investigation
# MAGIC %md
# MAGIC ## Scene 2: High Churn Risk Investigation
# MAGIC
# MAGIC An enterprise customer has been flagged with an elevated churn risk score. The retention team needs the Profile Agent to assemble a complete picture: what's driving the risk, what's the revenue exposure, and what cross-source signals indicate the customer is considering leaving.
# MAGIC
# MAGIC **Why this matters for CustomerLake:**
# MAGIC - A legacy CDP can flag churn risk from a single model. CustomerLake correlates across billing disputes (TMF), declining engagement (interactions), pipeline stalls (Salesforce), and payment delays (Oracle ERP).
# MAGIC - Only cross-source investigation reveals the full story: the customer has open disputes AND a declining CSAT AND their contract renewal is approaching with no active opportunity.
# MAGIC - This is the kind of investigation a human analyst would take hours to do manually across systems. The Profile Agent does it in seconds.

# COMMAND ----------

# DBTITLE 1,Run Scene 2 — Churn risk investigation
# Scene 2: Investigate a high-churn-risk customer.
# Find an active enterprise customer with high churn risk and multiple sources.
churn_candidate = spark.sql("""
    SELECT entity_id, name, customer_segment, churn_risk_score, arpu_tier,
           source_count, lifecycle_status
    FROM cdm_tmforum.gold.customer_profile_360
    WHERE customer_segment = 'enterprise'
      AND source_count >= 3
    ORDER BY churn_risk_score DESC
    LIMIT 1
""").collect()[0]

print(f"Selected churn risk candidate: entity={churn_candidate.entity_id}, "
      f"name={churn_candidate.name}, churn_risk={churn_candidate.churn_risk_score}, "
      f"arpu={churn_candidate.arpu_tier}, sources={churn_candidate.source_count}")

scene_2 = run_profile_agent(
    f"""URGENT: Enterprise customer entity {churn_candidate.entity_id} has been flagged 
    with a churn risk score of {churn_candidate.churn_risk_score}. This is a 
    {churn_candidate.arpu_tier} ARPU customer in the {churn_candidate.customer_segment} segment.
    
    The retention team needs a complete investigation:
    
    1. Pull the full golden record to understand this customer
    2. Check all source records to understand the breadth of our data
    3. Get Salesforce details: Are there active contracts? Upcoming renewals? 
       Open opportunities or stalled pipeline?
    4. Get Oracle ERP details: What's the actual revenue? Any AR issues?
    5. Get billing summary: Any disputes? Payment delays? Outstanding balances?
    6. Get interaction history: Recent support contacts? Escalations? CSAT trends?
    7. Get entity relationships: Is this part of a larger org? What's the total 
       relationship value?
    
    Produce an enrichment report that:
    - Quantifies the revenue at risk
    - Identifies specific churn signals across ALL source systems
    - Highlights cross-source correlations (e.g., billing disputes + declining CSAT + 
      stalled renewals = strong churn signal)
    - Recommends specific retention actions based on the evidence
    - Shows what a legacy CDP would MISS that CustomerLake can see
    """)

# COMMAND ----------

# DBTITLE 1,Scene 3 — Profile Completeness Audit
# MAGIC %md
# MAGIC ## Scene 3: Profile Completeness Audit — Enterprise Segment
# MAGIC
# MAGIC The data steward wants the Profile Agent to audit enterprise customer profiles for completeness gaps. Which profiles are most incomplete? Which source systems could fill the gaps? What's the business impact of incomplete profiles?
# MAGIC
# MAGIC **Why this matters for CustomerLake:**
# MAGIC - A legacy CDP reports completeness as a percentage. CustomerLake's Profile Agent diagnoses *which* attributes are missing, *which source system* has the data, and *what business impact* the gap creates.
# MAGIC - Cross-source gap analysis is impossible without unified identity: you need to know that entity 15143's phone is in TMF but missing in Salesforce, and that the gap is blocking a campaign targeting.
# MAGIC - The agent produces an actionable enrichment plan, not just a metric.

# COMMAND ----------

# DBTITLE 1,Run Scene 3 — Batch profile completeness audit
scene_3 = run_profile_agent(
    """You are performing a data quality audit for the enterprise customer segment.
    
    The CMO has asked: "How complete are our enterprise customer profiles? 
    Which ones have gaps that are blocking campaign targeting or churn prediction?"
    
    Your investigation:
    
    1. Run a batch completeness analysis on enterprise segment profiles
    2. For the 3 most incomplete profiles, investigate:
       a. Which specific attributes are missing?
       b. Which source systems are linked? 
       c. Which source systems COULD fill the gaps?
       d. What's the business impact of each gap?
    3. Check survivorship history for one profile to show how values were selected
    
    Produce an audit report that:
    - Summarizes enterprise segment completeness overall
    - Deep-dives into the worst profiles with specific gap analysis
    - Maps each gap to a source system that could fill it
    - Quantifies business impact (e.g., "missing email blocks 3 campaign audiences")
    - Recommends a prioritized enrichment plan
    - Highlights what makes this analysis impossible without CustomerLake's unified identity
    """)

# COMMAND ----------

# DBTITLE 1,Differentiation Summary
# MAGIC %md
# MAGIC ## CustomerLake Profile Agent — Differentiation Summary
# MAGIC
# MAGIC | Capability | Legacy CDP (Braze/Iterable/Salesforce) | CustomerLake Profile Agent |
# MAGIC |---|---|---|
# MAGIC | **Enrichment timing** | Batch ETL (nightly/hourly) | On-demand, real-time investigation |
# MAGIC | **Source integration** | Pre-configured connectors, one-way sync | Queries across ALL lakehouse sources via identity graph |
# MAGIC | **Conflict resolution** | Static priority rules ("CRM always wins") | AI-driven reasoning over recency, authority, completeness |
# MAGIC | **Provenance** | No visibility into which source contributed what | Full source attribution for every attribute value |
# MAGIC | **Gap analysis** | Percentage completeness metric | Per-attribute gap diagnosis with source-to-fill mapping |
# MAGIC | **Cross-source signals** | Impossible — data lives in silos | Correlates billing disputes + CSAT decline + pipeline stalls across systems |
# MAGIC | **Survivorship explanation** | Opaque merge logic | Full audit trail: which value won, from which source, and why |
# MAGIC | **Business impact** | Generic data quality scores | Revenue-at-risk quantification, campaign-blocking gap identification |
# MAGIC | **Scalability** | Fixed schema, pre-built connectors | Any Unity Catalog table becomes an enrichment source |

# COMMAND ----------

# DBTITLE 1,Summary statistics — agent performance across scenes
# Summarize agent performance across all scenes
scenes = {
    "Scene 1: New CRM Contact Enrichment": scene_1,
    "Scene 2: Churn Risk Investigation": scene_2,
    "Scene 3: Profile Completeness Audit": scene_3,
}

print("\n" + "="*70)
print("PROFILE AGENT — PERFORMANCE SUMMARY")
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
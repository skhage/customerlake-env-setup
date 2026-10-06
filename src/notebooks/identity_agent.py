# Databricks notebook source
# DBTITLE 1,CustomerLake Identity Resolution Agent
# MAGIC %md
# MAGIC # CustomerLake Identity Resolution Agent
# MAGIC
# MAGIC **Purpose:** Demonstrate CustomerLake's Agentic Identity Resolution — the ability to investigate ambiguous source records, assemble cross-source evidence, and produce explainable merge/split recommendations with confidence scores.
# MAGIC
# MAGIC **What legacy CDPs can't do:**
# MAGIC - Legacy CDPs apply static rule cascades and produce binary match/no-match — with no explanation.
# MAGIC - CustomerLake's Identity Agent **reasons over conflicting signals**, weighs evidence from every source system, and produces a structured recommendation a steward can act on.
# MAGIC - The agent accesses the full identity graph (97.8K match decisions, 60K identifier assertions, 16K candidate pairs) to find patterns no single rule would catch.
# MAGIC
# MAGIC **Demo scenarios:**
# MAGIC 1. **Merger/divestiture resolution** — Two org records share a name root but have conflicting DUNS numbers (post-acquisition name change).
# MAGIC 2. **High-reuse email triage** — An email address appears across 5+ entities; the agent must determine which link is genuine.
# MAGIC 3. **Bulk review queue triage** — The agent processes multiple review-queue cases in batch, prioritizing by business impact.
# MAGIC
# MAGIC **Architecture:** Foundation Model API → SQL tools over `identity.*` layer → structured JSON recommendation → steward approval.

# COMMAND ----------

# DBTITLE 1,Setup — imports and configuration
import json, textwrap, uuid
from datetime import datetime
from openai import OpenAI
from pyspark.sql import functions as F

# --- Configuration ---
CATALOG = "cdm_tmforum"
MODEL_ENDPOINT = "databricks-claude-sonnet-4-5"  # Best reasoning for evidence weighing
MAX_TURNS = 8  # Agent reasoning loop limit

# Foundation Model API client (OpenAI-compatible)
client = OpenAI(
    api_key=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    base_url=f"{spark.conf.get('spark.databricks.workspaceUrl').rstrip('/')}/serving-endpoints"
)

print(f"Identity Resolution Agent configured")
print(f"  Model: {MODEL_ENDPOINT}")
print(f"  Catalog: {CATALOG}")
print(f"  Identity layer: {CATALOG}.identity (35.7K entities, 97.8K decisions, 60K identifiers)")

# COMMAND ----------

# DBTITLE 1,Tool definitions — SQL functions the agent calls
# ---------------------------------------------------------------------------
# Identity Resolution Agent tools
# Each tool runs a scoped SQL query against the identity layer and returns
# structured JSON the agent can reason over.
# ---------------------------------------------------------------------------

def get_review_queue_cases(limit: int = 5, reason_filter: str = None) -> str:
    """Fetch cases from the identity decision review queue."""
    where = "WHERE 1=1"
    if reason_filter:
        where += f" AND reason_code LIKE '%{reason_filter}%'"
    df = spark.sql(f"""
        SELECT decision_id, decision, reason_code, reason_detail,
               score, blocking_rule, pair_id,
               left_source, left_record_id, right_source, right_record_id,
               supporting_evidence, conflicting_evidence
        FROM {CATALOG}.identity.v_decision_review_queue
        {where}
        ORDER BY score DESC
        LIMIT {limit}
    """)
    return json.dumps([row.asDict() for row in df.collect()], default=str)


def get_entity_profile(entity_id: str) -> str:
    """Get full identity profile for an entity: registry, xrefs, identifiers."""
    registry = spark.sql(f"""
        SELECT * FROM {CATALOG}.identity.entity_registry
        WHERE entity_id = '{entity_id}'
    """).collect()

    xrefs = spark.sql(f"""
        SELECT xref_id, source_instance, object_type, source_record_id,
               match_rule, confidence, is_current
        FROM {CATALOG}.identity.entity_xref
        WHERE entity_id = '{entity_id}' AND is_current = true
    """).collect()

    identifiers = spark.sql(f"""
        SELECT identifier_namespace, identifier_token,
               verification_status, source_instance
        FROM {CATALOG}.identity.identifier_assertion
        WHERE entity_id = '{entity_id}'
    """).collect()

    decisions = spark.sql(f"""
        SELECT decision_id, decision, reason_code, reason_detail,
               actor, decided_at
        FROM {CATALOG}.identity.match_decision
        WHERE xref_id IN (
            SELECT xref_id FROM {CATALOG}.identity.entity_xref
            WHERE entity_id = '{entity_id}' AND is_current = true
        )
        ORDER BY decided_at DESC
        LIMIT 10
    """).collect()

    return json.dumps({
        "entity": [r.asDict() for r in registry],
        "crossreferences": [r.asDict() for r in xrefs],
        "identifiers": [r.asDict() for r in identifiers],
        "recent_decisions": [r.asDict() for r in decisions]
    }, default=str)


def get_candidate_pair_evidence(pair_id: str) -> str:
    """Get full evidence for a specific candidate pair."""
    pair = spark.sql(f"""
        SELECT * FROM {CATALOG}.identity.candidate_pair
        WHERE pair_id = '{pair_id}'
    """).collect()

    decisions = spark.sql(f"""
        SELECT decision_id, decision, reason_code, reason_detail,
               actor, decided_at, rule_version, evidence_refs
        FROM {CATALOG}.identity.match_decision
        WHERE pair_id = '{pair_id}'
        ORDER BY decided_at DESC
    """).collect()

    return json.dumps({
        "candidate_pair": [r.asDict() for r in pair],
        "decisions": [r.asDict() for r in decisions]
    }, default=str)


def get_identifier_conflicts(entity_id: str) -> str:
    """Check if any of this entity's identifiers conflict with other entities."""
    df = spark.sql(f"""
        SELECT c.identifier_namespace, c.identifier_token,
               c.entity_count, c.entity_ids,
               c.entity_type, c.entity_status
        FROM {CATALOG}.identity.v_identifier_conflicts c
        WHERE array_contains(c.entity_ids, '{entity_id}')
    """)
    rows = df.dropDuplicates(["identifier_namespace", "identifier_token"]).collect()
    return json.dumps([r.asDict() for r in rows], default=str)


def get_entity_relationships(entity_id: str) -> str:
    """Get typed relationships from the gold layer for business context."""
    df = spark.sql(f"""
        SELECT relationship_type, from_entity_id, to_entity_id,
               from_role, to_role, source_instance, is_current
        FROM {CATALOG}.gold.typed_relationship
        WHERE (from_entity_id = '{entity_id}' OR to_entity_id = '{entity_id}')
          AND is_current = true
        LIMIT 20
    """)
    return json.dumps([r.asDict() for r in df.collect()], default=str)


def get_low_confidence_memberships(entity_id: str) -> str:
    """Get low-confidence source memberships that may need review."""
    df = spark.sql(f"""
        SELECT xref_id, source_instance, object_type, source_record_id,
               match_rule, confidence, decision, reason_code, reason_detail
        FROM {CATALOG}.identity.v_low_confidence_memberships
        WHERE entity_id = '{entity_id}'
    """)
    return json.dumps([r.asDict() for r in df.collect()], default=str)


# Tool registry for the agent
TOOLS = {
    "get_review_queue_cases": get_review_queue_cases,
    "get_entity_profile": get_entity_profile,
    "get_candidate_pair_evidence": get_candidate_pair_evidence,
    "get_identifier_conflicts": get_identifier_conflicts,
    "get_entity_relationships": get_entity_relationships,
    "get_low_confidence_memberships": get_low_confidence_memberships,
}

# OpenAI function-calling schema
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_review_queue_cases",
            "description": "Fetch cases from the identity decision review queue. These are candidate pairs that the deterministic resolver flagged for human review due to conflicting evidence.",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max cases to return (default 5)"},
                    "reason_filter": {"type": "string", "description": "Filter by reason_code substring (e.g. 'duns_conflict', 'email', 'reuse')"}
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_entity_profile",
            "description": "Get the full identity profile for an entity: registry info, all source crossreferences, identifier assertions, and recent match decisions. Use this to understand an entity before recommending merge/split.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "description": "The entity_id to look up"}
                },
                "required": ["entity_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_candidate_pair_evidence",
            "description": "Get full evidence (comparison vector, supporting/conflicting evidence, decisions) for a specific candidate pair.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pair_id": {"type": "string", "description": "The pair_id to look up"}
                },
                "required": ["pair_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_identifier_conflicts",
            "description": "Check if any of this entity's identifiers (email, phone, DUNS, tax_id) conflict with other entities. Returns cases where the same identifier token is claimed by multiple entities.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "description": "The entity_id to check"}
                },
                "required": ["entity_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_entity_relationships",
            "description": "Get typed relationships (parent-subsidiary, customer-provider, partner, etc.) for an entity from the gold layer. Useful for understanding business context and detecting transitive relationships.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "description": "The entity_id to look up"}
                },
                "required": ["entity_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_low_confidence_memberships",
            "description": "Get source-to-entity crossreferences with low confidence scores that may need review. Shows which source records are weakly linked to this entity.",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "description": "The entity_id to check"}
                },
                "required": ["entity_id"]
            }
        }
    }
]

print(f"Registered {len(TOOLS)} identity resolution tools")
for name in TOOLS:
    print(f"  \u2022 {name}")

# COMMAND ----------

# DBTITLE 1,Identity Resolution Agent — core agent loop
# ---------------------------------------------------------------------------
# Identity Resolution Agent
# Uses Foundation Model API with tool calling to investigate ambiguous
# identity cases and produce structured merge/split recommendations.
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = textwrap.dedent("""\
    You are the CustomerLake Identity Resolution Agent. Your job is to investigate
    ambiguous identity cases and produce structured merge/split recommendations
    with full evidence trails.

    ## Your capabilities
    You have access to the full identity graph in Unity Catalog:
    - 35,671 resolved entities (organizations and individuals)
    - 97,783 match decisions with full audit trail
    - 60,000 identifier assertions across 6 namespaces
    - 16,107 candidate pairs with comparison vectors
    - 254,209 typed relationships in the gold layer

    ## Your tools
    - get_review_queue_cases: Find cases flagged for review
    - get_entity_profile: Full identity profile (xrefs, identifiers, decisions)
    - get_candidate_pair_evidence: Evidence for a specific candidate pair
    - get_identifier_conflicts: Check for identifier conflicts
    - get_entity_relationships: Business context from typed relationships
    - get_low_confidence_memberships: Weak source links needing review

    ## Investigation protocol
    1. ALWAYS start by examining both sides of a candidate pair
    2. Pull entity profiles for BOTH entities involved
    3. Check for identifier conflicts that could explain the ambiguity
    4. Review business relationships for context (parent-subsidiary, etc.)
    5. Examine low-confidence memberships for weak links

    ## Output format
    After investigation, produce a structured recommendation:
    ```
    RECOMMENDATION: [MERGE | SPLIT | NO_ACTION | ESCALATE]
    CONFIDENCE: [0.0 - 1.0]
    ENTITIES: [list of entity_ids involved]
    EVIDENCE FOR: [supporting signals]
    EVIDENCE AGAINST: [conflicting signals]
    REASONING: [step-by-step explanation]
    BUSINESS IMPACT: [what happens if we get this wrong]
    NEXT STEPS: [what the steward should verify]
    ```

    ## Rules
    - Organizations and individuals are SEPARATE domains — never cross-merge
    - Common parent must NOT boost two subsidiaries into the same entity
    - Shared email/phone is supporting context, NEVER sufficient alone
    - When evidence is ambiguous, recommend ESCALATE over a risky merge
    - Always explain your reasoning — a steward must be able to follow your logic
""")


def run_identity_agent(user_message: str, verbose: bool = True) -> dict:
    """Run the Identity Resolution Agent with tool calling."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message}
    ]

    if verbose:
        print(f"\n{'='*70}")
        print(f"IDENTITY AGENT — Investigation started")
        print(f"{'='*70}")
        print(f"Query: {user_message[:200]}")

    for turn in range(MAX_TURNS):
        response = client.chat.completions.create(
            model=MODEL_ENDPOINT,
            messages=messages,
            tools=TOOL_SCHEMAS,
            max_tokens=4096,
        )
        choice = response.choices[0]

        # If the model wants to call tools
        if choice.finish_reason == "tool_calls" and choice.message.tool_calls:
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)
                if verbose:
                    print(f"\n  \u21B3 Tool call [{turn+1}]: {fn_name}({fn_args})")
                try:
                    result = TOOLS[fn_name](**fn_args)
                    # Truncate very large results for the LLM context
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
            # Final response
            final_text = choice.message.content
            if verbose:
                print(f"\n{'='*70}")
                print("AGENT RECOMMENDATION")
                print(f"{'='*70}")
                print(final_text)
            return {
                "recommendation": final_text,
                "turns": turn + 1,
                "messages": messages
            }

    return {"recommendation": "MAX_TURNS reached", "turns": MAX_TURNS, "messages": messages}


print("Identity Resolution Agent ready.")

# COMMAND ----------

# DBTITLE 1,Scene 1 — Merger/Divestiture DUNS Conflict
# MAGIC %md
# MAGIC ## Scene 1: Merger/Divestiture — DUNS Conflict Resolution
# MAGIC
# MAGIC A CRM account (`ACCT-MRG-PRE`) matched to entity `10092` by name root, but their DUNS numbers differ. This is the classic post-acquisition scenario: a company changed its DUNS after a merger, but the CRM still has the old one.
# MAGIC
# MAGIC **Why this matters for CustomerLake:**
# MAGIC - A legacy CDP would either blindly merge (wrong — corrupts the org graph) or blindly reject (wrong — loses the link).
# MAGIC - CustomerLake's Identity Agent investigates the full evidence: identifiers, relationships, other source corroboration, and produces an **explainable recommendation** a steward can act on.

# COMMAND ----------

# DBTITLE 1,Run Scene 1 — Merger DUNS conflict investigation
scene_1 = run_identity_agent(
    """Investigate this identity case from the review queue:

    A candidate pair (pair_id: pair-s12-1b5b1e4ea132) was flagged for review.
    Left side: MOCK_CRM account 'ACCT-MRG-PRE' 
    Right side: MOCK_DNB organization 'DUNS-690671863' (entity 10092)
    Blocking rule: name_root_match (score 0.85)
    Supporting evidence: Name root matches, both are organizations
    Conflicting evidence: Different DUNS numbers, parent_duns differs
    
    This appears to be a merger/divestiture scenario. Investigate fully:
    1. Get the candidate pair evidence
    2. Profile both entities involved
    3. Check for identifier conflicts
    4. Examine business relationships for M&A context
    5. Produce a structured recommendation"""
)

# COMMAND ----------

# DBTITLE 1,Scene 2 — High Email Reuse Triage
# MAGIC %md
# MAGIC ## Scene 2: High Email Reuse — Multi-Entity Disambiguation
# MAGIC
# MAGIC Shared email addresses are one of the hardest identity resolution problems. A single email like `melissacarter@example.org` appears across 7 different entities. The agent must determine which link is genuine and which are coincidental reuse (role inboxes, shared accounts, data entry errors).
# MAGIC
# MAGIC **CustomerLake differentiation:**
# MAGIC - Legacy CDPs treat email as a deterministic key — same email = same person. This creates **false merges** that corrupt audience segments and inflate campaign reach.
# MAGIC - The Identity Agent examines corroborating signals (name match, phone match, company registration, relationship graph) to determine which email-entity link is genuine.

# COMMAND ----------

# DBTITLE 1,Run Scene 2 — Email reuse disambiguation
scene_2 = run_identity_agent(
    """Investigate a high-reuse email conflict:
    
    The email 'melissacarter@example.org' is claimed by 7 different entities: 
    [12818, 13319, 14030, 14980, 16124, 19446, 19816]
    All are organizations with 'active' status.
    
    This is suspicious — 7 different organizations sharing one email suggests either:
    (a) Some of these entities should be merged (they're the same org)
    (b) The email is a role inbox shared across related orgs
    (c) Data entry error (same email used for unrelated orgs)
    
    Investigate:
    1. Check identifier conflicts for entity 12818 (the first entity)
    2. Profile entities 12818 and 13319 to compare their source data
    3. Check if any of these entities have relationships to each other
    4. Look at low-confidence memberships for the entities
    5. Recommend which links are genuine and which should be severed"""
)

# COMMAND ----------

# DBTITLE 1,Scene 3 — Batch Review Queue Triage
# MAGIC %md
# MAGIC ## Scene 3: Batch Review Queue Triage — Prioritized Case Processing
# MAGIC
# MAGIC In production, stewards face hundreds of review-queue cases. The Identity Agent triages them in batch: categorizing by type, estimating business impact, and prioritizing which cases to resolve first.
# MAGIC
# MAGIC **CustomerLake differentiation:**
# MAGIC - Legacy CDPs generate match reports but leave prioritization to the steward.
# MAGIC - CustomerLake's Identity Agent **reasons about business impact**: a duplicate in a Platinum-tier account matters more than a duplicate in a dormant account. It prioritizes by revenue-at-risk, not just match score.

# COMMAND ----------

# DBTITLE 1,Run Scene 3 — Batch review queue triage
scene_3 = run_identity_agent(
    """You are triaging the identity review queue. 
    
    Fetch 5 cases from the review queue that have DUNS conflicts 
    (reason_filter: 'duns_conflict').
    
    For each case:
    1. Categorize it (merger, subsidiary confusion, data error, etc.)
    2. Assess business impact (high/medium/low) based on the entities involved
    3. Recommend action priority
    
    Then produce a prioritized triage report:
    - Which cases should a steward resolve first?
    - Which cases can be auto-resolved with high confidence?
    - Which cases need additional data before a decision can be made?
    
    Format the output as a structured triage report the steward team can act on."""
)

# COMMAND ----------

# DBTITLE 1,Differentiation Summary
# MAGIC %md
# MAGIC ## CustomerLake Identity Resolution — Differentiation Summary
# MAGIC
# MAGIC | Capability | Legacy CDP (Braze/Iterable/Salesforce) | CustomerLake Identity Agent |
# MAGIC |---|---|---|
# MAGIC | **Matching** | Static rule cascades (email → phone → name) | Multi-signal reasoning with conflict weighing |
# MAGIC | **Explainability** | Match score only | Full evidence trail with structured reasoning |
# MAGIC | **M&A handling** | Manual steward re-keying | Agent detects DUNS changes, proposes merge with context |
# MAGIC | **Email reuse** | False merges (same email = same person) | Corroboration analysis across identifiers |
# MAGIC | **Prioritization** | Flat match-score ranking | Business-impact-aware triage (revenue, tier, recency) |
# MAGIC | **Steward workflow** | Export CSV for manual review | Structured recommendations stewards can approve/reject |
# MAGIC | **Scale** | Thousands of rules to maintain | Agent reasons over 97K+ historical decisions as precedent |
# MAGIC | **Audit trail** | Limited | Full decision lineage: who decided, when, why, what evidence |
# MAGIC
# MAGIC **Bottom line:** CustomerLake doesn't just match records — it **investigates** identity cases with the same rigor a human analyst would, but at scale. A CMO can trust that audience segments are built on correctly resolved identities, not false merges.

# COMMAND ----------

# DBTITLE 1,Summary statistics — agent performance
# Summarize agent performance across all scenes
scenes = {
    "Scene 1: Merger DUNS Conflict": scene_1,
    "Scene 2: Email Reuse Disambiguation": scene_2,
    "Scene 3: Batch Queue Triage": scene_3,
}

print("\n" + "="*70)
print("IDENTITY AGENT — SESSION SUMMARY")
print("="*70)
for name, result in scenes.items():
    tool_calls = sum(
        1 for m in result["messages"]
        if hasattr(m, 'tool_calls') and m.tool_calls
        for _ in m.tool_calls
    )
    rec_lines = result["recommendation"].split("\n")
    # Extract recommendation type if present
    rec_type = "N/A"
    confidence = "N/A"
    for line in rec_lines:
        if "RECOMMENDATION:" in line:
            rec_type = line.split("RECOMMENDATION:")[1].strip()
        if "CONFIDENCE:" in line:
            confidence = line.split("CONFIDENCE:")[1].strip()
    print(f"\n{name}")
    print(f"  Turns: {result['turns']} | Tool calls: {tool_calls}")
    print(f"  Recommendation: {rec_type} | Confidence: {confidence}")

print(f"\n{'='*70}")
print("Total identity layer coverage:")
print(f"  Entities: 35,671 | Decisions: 97,783 | Identifiers: 60,000")
print(f"  Candidate pairs: 16,107 | Relationships: 254,209")
print(f"  Review queue depth: 87,027 cases")
print(f"{'='*70}")
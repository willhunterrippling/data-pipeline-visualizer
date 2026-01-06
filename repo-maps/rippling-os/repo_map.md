# Repo Map: rippling-os

## What This Repo Is

- **SQL Query Toolkit** for analyzing Rippling's marketing/sales pipeline data from Snowflake
- **NOT a dbt project** — queries data from `prod_rippling_dwh` and `prod_dbt_db` (dbt lives elsewhere)
- Python CLI (`core/query_runner.py`) + reusable SQL functions + schema documentation

## Where the Data Stuff Lives

| Directory | Purpose |
|-----------|---------|
| `core/sql_functions/` | Reusable Snowflake UDFs (analyze_opp, get_sequence_type, get_sequence_volume) |
| `core/references/` | Schema docs & metric definitions — **source of truth** |
| `sql/` | Template SQL for recurring patterns (audience building) |
| `temp/` | Ad-hoc analysis queries and outputs |
| `core/clients/snowflake/` | Python Snowflake client with SSO auth |

## Primary Data Sources

```
prod_rippling_dwh.outreach    — Outreach.io (sequences, prospects, mailings)
prod_rippling_dwh.sfdc        — Salesforce (opportunities, accounts, contacts, leads)
prod_rippling_dwh.growth      — Growth team tables (mechanized_outreach_population)
prod_dbt_db.core_growth       — dbt-generated growth models
```

> **Citation:** [`core/references/SNOWFLAKE_TABLES.md:14-65`](core/references/SNOWFLAKE_TABLES.md)

## Key SQL Functions

### `analyze_opp.sql`
Links opportunities to Outreach sequences with **45-day attribution window**.

```sql
-- Core join pattern (lines 72-77)
JOIN prod_rippling_dwh.outreach.data_connection dc
    ON dc.parent_id = ss.relationship_prospect_id 
    AND dc.type IN ('Contact', 'Lead')
WHERE ABS(DATEDIFF('day', ss.created_at, opp_created_date)) <= 45
```

> **Citation:** [`core/sql_functions/analyze_opp.sql:72-77`](core/sql_functions/analyze_opp.sql)

### `get_sequence_type.sql`
Classifies sequences: CANNON, AUTOBOUND, CLASSIC_MO_PERSONALIZED, etc.

> **Citation:** [`core/sql_functions/get_sequence_type.sql:56-144`](core/sql_functions/get_sequence_type.sql)

### `get_sequence_volume.sql`
Calculates unique prospects + total sends for sequences within date range.

> **Citation:** [`core/sql_functions/get_sequence_volume.sql:1-97`](core/sql_functions/get_sequence_volume.sql)

## Key Conventions

### S1/S2 Definitions (CRITICAL)
| Stage | Definition | SQL Pattern |
|-------|------------|-------------|
| **S1** | ANY opportunity created | `WHERE is_deleted = FALSE` (no stage filter!) |
| **S2** | SQO qualified | `WHERE sqo_qualified_date_c IS NOT NULL` |

> **Citation:** [`core/references/PIPELINE_METRICS_DEFINITIONS.md:19-75`](core/references/PIPELINE_METRICS_DEFINITIONS.md)

### Sequence Tags
- **Case-sensitive** — use exact: `'EmailProgram-MechOutreach'`
- Primary email outreach tag: `EmailProgram-MechOutreach` (1,717 sequences)
- 329 total tags as of 2024-12-11

> **Citation:** [`core/references/PIPELINE_METRICS_DEFINITIONS.md:78-134`](core/references/PIPELINE_METRICS_DEFINITIONS.md)

### SQL Patterns to Follow

| Pattern | Correct | Wrong |
|---------|---------|-------|
| Prospect→SFDC linking | `dc.type IN ('Contact', 'Lead')` | `dc.parent_type = 'prospect'` (field doesn't exist) |
| Attribution window | 45 days from `sequence_state.created_at` | Using enrollment date |
| Soft delete filtering | `is_deleted = FALSE AND _fivetran_deleted = FALSE` | Missing either filter |
| Tag matching | `tag_name = 'EmailProgram-MechOutreach'` | `LOWER(tag_name) = '...'` |

## Grouping Rules for Lineage

1. **By data source schema**: `outreach.*`, `sfdc.*`, `growth.*`
2. **By sequence tag**: `EmailProgram-MechOutreach`, `EmailProgram-DirectMail`, etc.
3. **By pipeline stage**: S1 (all opps), S2 (SQO qualified)

## Flow Candidates

### 1. Mechanized Outreach Attribution (Primary)
**Path:** MO Population → Sequence State → Mailing → Data Connection → Opportunity

**Anchor tables:**
- `prod_rippling_dwh.growth.mechanized_outreach_population`
- `prod_rippling_dwh.outreach.sequence_state`
- `prod_rippling_dwh.outreach.data_connection`
- `prod_rippling_dwh.sfdc.opportunity`

> **Citation:** [`core/sql_functions/analyze_opp.sql:1-129`](core/sql_functions/analyze_opp.sql)

### 2. Audience Building (Cannon)
**Path:** MO Population + SFDC Account/Lead + CoreSignal → Audience CSV

> **Citation:** [`sql/audience__cannon_template.sql:1-44`](sql/audience__cannon_template.sql)

## Ambiguities / Gotchas

| Issue | Impact | Next Step |
|-------|--------|-----------|
| dbt models are external | Can't trace full lineage from raw sources | Get manifest.json from dbt repo |
| Hardcoded sequence IDs in `get_sequence_type.sql` | May become stale | Consider tag-based classification |
| Tag names are case-sensitive | Queries fail silently on mismatch | Run tag discovery query periodically |
| S1/S2 ≠ stage names | Using stage names gives wrong counts | Always check `PIPELINE_METRICS_DEFINITIONS.md` |

## External dbt Architecture (Reference Only)

The dbt project (in separate repo) follows this structure:

```
Fivetran → Raw Schemas (prod_rippling_dwh)
         → Staging (stg_sfdc__*, stg_mongo_core__*)
         → Intermediate (int_sales__*, int_marketing__*)
         → Marts (Sales, Finance, Marketing, Growth...)
         → Looker / Tableau / Census
```

> **Citation:** [`data_map/dbt.md:1-182`](data_map/dbt.md)

---

*Generated: 2026-01-06 | See `repo_map.json` for machine-readable version*


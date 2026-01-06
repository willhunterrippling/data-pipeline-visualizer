# Rippling dbt Repository Map

## What This Repo Is

- **Enterprise dbt project** (`rippling_dbt`) targeting Snowflake with 30+ data sources
- **Domain-driven architecture**: Sales, Growth, Finance, Bookings, Marketing, Customer Experience
- **Layered transformations**: Sources → Staging → Intermediate → Marts/Reports

## Where Data Assets Live

| Asset Type | Location | Count |
|------------|----------|-------|
| Staging models | `models/sources/` | ~1261 SQL files |
| Mart models | `models/marts/` | ~1625 SQL files |
| Macros | `macros/` | ~140 SQL files |
| Snapshots | `snapshots/` | ~50 SQL files |
| Seeds | `seeds/` | ~43 CSV files |
| Tests | `tests/` | 32 SQL files |

## dbt Summary

### Compilation

```bash
# Setup (first time or after reset)
make setup
dbt seed

# Compile to generate manifest
dbt compile

# Build specific model
dbt build -s model_name

# Lint changes
make lint
```

### Key Files

| File | Purpose |
|------|---------|
| [`dbt_project.yml`](dbt_project.yml) | Project config, schema/tag definitions |
| [`packages.yml`](packages.yml) | Dependencies (dbt_utils, elementary, dbt_snow_mask) |
| [`profiles.yml`](profiles.yml) | Snowflake connection (dev/dev_prod targets) |
| `target/manifest.json` | Generated DAG artifact |

### Key Sources

| Source | Schema | Key Tables | Citation |
|--------|--------|------------|----------|
| SFDC | `SFDC` | ACCOUNT, CONTACT, LEAD, OPPORTUNITY | `models/sources/sfdc/sources.yml` |
| MONGO_CORE | `mongo_core` | COMPANY, ROLE_WITH_COMPANY, INVOICE | `models/sources/mongo_core/sources.yml` |
| MECH_OUTREACH | `MECH_OUTREACH` | APOLLO_LEAD_ENRICHMENT, ZOOMINFO_* | `models/sources/mech_outreach/` |
| OUTREACH | `OUTREACH` | SEQUENCE, SEQUENCE_STATE, PROSPECT | `models/sources/outreach/sources.yml` |

### Key Macros

| Macro | Purpose | Citation |
|-------|---------|----------|
| `{{ lookback_filter() }}` | Limits dev data to 30 days | `macros/common/lookback_filter.sql` |
| `{{ simple_cte([...]) }}` | Generates WITH clause from refs | `macros/common/simple_cte.sql` |
| `{{ generate_schema_name() }}` | Custom schema naming | `macros/data_engg/generate_schema_name.sql` |
| `{{ remove_deleted_records() }}` | Filters soft-deleted rows | Used in staging models |

## Key Conventions

### Naming Patterns

| Layer | Pattern | Example |
|-------|---------|---------|
| Staging | `stg_{source}__{table}` | `stg_sfdc__accounts` |
| Intermediate | `int_{domain}__{desc}` | `int_sales__opportunities` |
| Mart | `mart_{domain}__{desc}` | `mart_growth__lsw_lead_data` |
| Report | `rpt_{domain}__{desc}` | `rpt_sales__sdr_sales_compensation` |
| Snapshot | `ss_{domain}__{table}` | `ss_mongo_core__customer_hris_metrics` |

### Schema Mapping

- **Staging**: `{target}_stage` (e.g., `core_whunter_stage`)
- **Intermediate**: `{target}_{domain}_stage` (e.g., `core_whunter_sales_stage`)
- **Marts**: `{target}_{domain}` (e.g., `core_whunter_sales`)
- **Restricted**: Raw schema name (e.g., `product_restricted`)

## Grouping Rules for Lineage

1. **By Domain** - Primary grouping via directory and tags
   - `sales`, `growth`, `finance`, `bookings`, `marketing`, `customer_experience`
   - Citation: `dbt_project.yml:149-345`

2. **By Layer** - Model prefix indicates transformation stage
   - `stg_` → `int_` → `mart_`/`rpt_`

3. **By Refresh Schedule** - Tags define frequency
   - `hourly`, `daily`, `every_6_hours`, `bookings_hourly`
   - Citation: `dbt_project.yml:243-279`

4. **By Priority** - Critical models tagged
   - `p1` = mission-critical
   - Citation: `dbt_project.yml:159`

5. **By Source System** - Staging grouped by origin
   - `sfdc`, `mongo_core`, `outreach`, `jira`, `google_sheets`

## Flow Candidates

### 1. Unified Lead Database (Mechanized Outreach)

**Anchor**: `mart_growth__lsw_lead_data`

Aggregates lead enrichment data from Apollo, ZoomInfo, Cognism, Clearbit with intent signals from G2, Gartner. Feeds email campaigns, direct mail, CRM enrichment.

**Key models**: `stg_mech_outreach__*` → `int_growth__*_lead_enrichment_output` → `mart_growth__lsw_lead_aggregate` → `mart_growth__lsw_lead_data`

**Citation**: [`docs/mechanized_outreach_data_flow.md`](docs/mechanized_outreach_data_flow.md)

### 2. Bookings Pipeline

**Anchor**: `mart_bookings__line_items_final`

P1 priority, hourly refresh. 5-step transformation for ARR/revenue calculations.

**Key models**: `int_bookings__line_items_step1` → `step2` → `step3` → `step4` → `step_final` → `mart_bookings__line_items_final`

**Citation**: `dbt_project.yml:159-170`, `models/marts/bookings/hourly/`

### 3. Sales Commission

**Anchor**: `int_sales__sales_commissions`

Rep compensation calculations feeding SDR, AM, and leadership reports.

**Key models**: `int_sales__sales_commissions` → `rpt_sales__*_sales_compensation`

**Citation**: `models/marts/sales/intermediate/`, `models/marts/sales/report/sales_commissions/`

### 4. Sales Opportunities

**Anchor**: `int_sales__opportunities`

Central opportunity data with product mix, stage history, splits.

**Citation**: `models/marts/sales/intermediate/`

### 5. Finance Revenue

**Anchor**: `models/marts/finance/`

115 intermediate models, 208 report models.

**Citation**: `models/marts/finance/`

## Orchestration

- **External**: Dagster orchestration via `env_var('DAGSTER_INSTANCE')` references
- **Not in repo**: DAG/job definitions are in a separate orchestration repository
- **Citation**: `dbt_project.yml:72-76`

## Ambiguities / Gotchas

| Issue | Impact | Next Step |
|-------|--------|-----------|
| Dynamic schema names | Output schema varies by target | Parse `generate_schema_name` macro |
| Dev lookback filter | Dev builds limited to 30 days | Set `dev_lookback_days` or `DBT_LOOKBACK_DAYS` for full data |
| External orchestration | Schedule/job definitions not visible | Locate Dagster repo |
| Masking policies | `dbt_snow_mask` post-hooks may hide columns | Review masking config |
| Legacy models | Custom post-hooks in `legacy_snowflake_repo_views/` | Check deprecation status |
| Incremental snapshots | Mart models acting as snapshots | Treat differently from native snapshots |

---

*Generated: 2026-01-06 | dbt version: ≥1.10.13 | Warehouse: Snowflake*

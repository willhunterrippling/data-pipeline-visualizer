# Growth Engineering Repo Map

## What This Repo Is

- **Growth Engineering data pipelines** for Rippling's sales/marketing GTM (Go-To-Market) data operations
- **Dagster orchestration** with **Databricks DLT** for data transformations (no dbt)
- **FastAPI services** for AI email generation, research assistant, leadflow, and related applications

## Where the Data Stuff Lives

| Directory | Purpose |
|-----------|---------|
| `growth_dagster/src/growth_pipelines/` | Dagster orchestration: definitions, jobs, components |
| `growth_dagster/src/growth_databricks/` | Databricks DLT transformation scripts (Python + SQL) |
| `growth_dagster/src/growth_pipelines/defs/growth/` | YAML asset definitions per domain |
| `growth_dagster/src/growth_pipelines/defs/jobs.yaml` | Job schedules with Jinja templates |
| `src/growth_main/apps/` | FastAPI application modules |
| `src/growth_jobs/jobs/` | Background job handlers |

## Dagster Summary

### How to Run

```bash
# Setup
make dagster-setup

# Start dev server (UI at http://localhost:3000)
make dagster-start

# Validate definitions
make dagster-validate
```

### Entry Point

```python
# growth_dagster/src/growth_pipelines/definitions.py
@definitions
def defs():
    return load_from_defs_folder(project_root=project_root)
```

### Key Jobs

| Job | Schedule | Asset Groups |
|-----|----------|--------------|
| `*_vendor_companies_s3_to_iceberg_job` | Every 30 min | vendor_companies_s3_dataloader_dlt_pipeline |
| `*_vendor_leads_s3_to_iceberg_job` | Every 30 min | vendor_leads_s3_dataloader_dlt_pipeline |
| `*_timeline_dlt_job` | Manual | timeline_summarization |
| `*_context_store_jobs` | Every 6 hours | context_store_gtm_outbound_template_sync |

### Key Components

| Component | Purpose |
|-----------|---------|
| `DatabricksDLTPipeline` | Manage DLT pipelines with file upload and monitoring |
| `DatabricksJob` | Submit Spark Python/Notebook tasks |
| `GrowthServiceJob` | Trigger Growth Service API endpoints |

## Key Conventions

### Schema Layers (Medallion Architecture)

```
S3/Fivetran → bronze → staging → silver → gold
```

- **bronze**: Raw ingested data (Auto Loader streaming)
- **staging**: Type conversion and column extraction
- **silver**: CDC-based deduplication (SCD Type 1)
- **gold**: Business-ready aggregations and AI enrichments

### Naming Patterns

| Element | Pattern | Example |
|---------|---------|---------|
| Catalogs | `{env}_growth_catalog` | `prod_growth_catalog` |
| Tables | `{schema}.{vendor}_{entity}` | `silver.zoominfo_company_cube` |
| Assets | `{prefix}_{vendor}_{entity}_dlt` | `growth_prod_apollo_leads_data_dlt` |
| Asset Groups | `{prefix}_{domain}_pipeline` | `growth_prod_vendor_companies_s3_dataloader_dlt_pipeline` |

### Template Variables

Asset names use Jinja templates that resolve based on `DAGSTER_INSTANCE` env var:

| Variable | Dev | Staging | Prod |
|----------|-----|---------|------|
| `{{ dagster_safe_bundle_prefix }}` | `growth_dev` | `growth_staging` | `growth_prod` |
| `{{ catalog_name }}` | `dev_growth_catalog` | `stage_growth_catalog` | `prod_growth_catalog` |
| `{{ environment }}` | `development` | `staging` | `production` |

## Grouping Rules for Lineage

Recommended grouping strategies for a pipeline viewer:

1. **By Vendor**: Group assets by data vendor (zoominfo, apollo, cognism, demandbase, g2, gartner, etc.)
2. **By Entity Type**: companies, leads, jobs, funding_rounds
3. **By Medallion Layer**: bronze, staging, silver, gold
4. **By Domain**: vendor_data, timeline_summarization, context_store

## Flow Candidates

### 1. Vendor Company Data Ingestion

```
S3 vendor buckets → bronze.{vendor}_companies → staging → silver
Fivetran (Crunchbase/Pitchbook) → bronze → staging → silver
```

- **Schedule**: Every 30 minutes
- **Anchor tables**: `silver.zoominfo_company_cube`, `silver.demandbase_companies`, etc.
- **Source**: `growth_dagster/src/growth_pipelines/defs/growth/dlt_vendor_companies_s3_dataloader_pipeline/defs.yaml`

### 2. Vendor Lead Data Ingestion

```
S3 vendor buckets → bronze.{vendor}_lead_* → silver → gold.apollo_leads_data
```

- **Schedule**: Every 30 minutes
- **Anchor tables**: `silver.zoominfo_lead_search`, `gold.apollo_leads_data`
- **Source**: `growth_dagster/src/growth_pipelines/defs/growth/dlt_vendor_leads_s3_dataloader_pipeline/defs.yaml`

### 3. Timeline Summarization (AI-Enriched)

```
silver.email_thread_summaries ─┐
silver.timeline_gong_calls ────┼─→ gold.interaction_timeline_event → gold.unified_interaction_timeline
silver.timeline_closed_lost ───┘                                      (LLM: Llama 3.3 70B)
```

- **Schedule**: Manual trigger
- **AI Model**: `databricks-meta-llama-3-3-70b-instruct`
- **Anchor table**: `gold.unified_interaction_timeline`
- **Source**: `growth_dagster/src/growth_databricks/timeline_summarization/transformations/unified_interaction_timeline.py`

### 4. Context Store Sync

```
Growth Service API → context_store_gtm_outbound_template_sync
```

- **Schedule**: Every 6 hours
- **Source**: `growth_dagster/src/growth_pipelines/defs/growth/context_store_gtm_outbound_template_sync/defs.yaml`

## Ambiguities / Gotchas

| Issue | Impact | Recommendation |
|-------|--------|----------------|
| **Dynamic asset names** | Asset keys contain `{{ dagster_safe_bundle_prefix }}` | Resolve by checking `DAGSTER_INSTANCE` env var |
| **Environment-specific catalogs** | Catalog names vary per env | Normalize or parameterize for cross-env lineage |
| **Hardcoded prod Fivetran refs** | SQL references `prod_fivetran_mdls_catalog` | May not work in dev/staging without catalog mapping |
| **Python-based table creation** | Most tables created via `DLTTableBuilder` class | Parse Python to extract schemas and dependencies |
| **Cross-platform Snowflake refs** | Timeline reads from `snowflake_us1_prod_*` via Unity Catalog | Map external connections for full lineage |

## Key File References

| Component | File | Lines |
|-----------|------|-------|
| Dagster entry | `growth_dagster/src/growth_pipelines/definitions.py` | 1-11 |
| Jobs config | `growth_dagster/src/growth_pipelines/defs/jobs.yaml` | 1-105 |
| Template vars | `growth_dagster/src/growth_pipelines/defs/template_vars.py` | 1-414 |
| DLT Builder | `growth_dagster/src/growth_databricks/vendor_s3_dataloader_dlt_pipeline/utilities/dlt_table_builder.py` | 1-713 |
| Timeline AI | `growth_dagster/src/growth_databricks/timeline_summarization/transformations/unified_interaction_timeline.py` | 1-622 |
| Company DLT def | `growth_dagster/src/growth_pipelines/defs/growth/dlt_vendor_companies_s3_dataloader_pipeline/defs.yaml` | 1-198 |
| Leads DLT def | `growth_dagster/src/growth_pipelines/defs/growth/dlt_vendor_leads_s3_dataloader_pipeline/defs.yaml` | 1-122 |

## Data Vendors Covered

**Companies**: Zoominfo, Demandbase, Cognism, Clearbit, G2, Gartner, TrustRadius, Coresignal, PredictLeads, Lusha, Pitchbook, Crunchbase

**Leads**: Zoominfo, Apollo, Cognism, Clearbit, Lusha, Kaspr, Pitchbook

**Funding/Jobs**: Crunchbase (funding rounds), Coresignal (jobs), PredictLeads (jobs)


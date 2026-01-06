# Pipeline Explorer

Interactive data pipeline visualization for Rippling's data infrastructure. Index dbt models, Airflow DAGs, and Snowflake metadata into an AI-powered graph explorer.

## Features

- **Multi-repo indexing**: Parse dbt manifests, Airflow DAGs, and SQL files
- **Interactive graph**: Cytoscape.js-powered visualization with groups, zoom/pan, minimap
- **Smart grouping**: AI-inferred groups based on domains, layers, naming conventions
- **Flow discovery**: Automatically detect data flows (Mechanized Outreach, Bookings, etc.)
- **AI explanations**: On-demand plain-English explanations for tables/models
- **Search**: Full-text search across all nodes
- **Column lineage**: On-demand column-level lineage extraction
- **Side panel**: Upstream/downstream navigation, metadata, citations

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Configure environment

Copy the example and fill in your paths:

```bash
cp .env.local.example .env.local
```

Edit `.env.local`:

```bash
# Repo paths (use absolute paths or ~)
RIPPLING_DBT_PATH=~/Documents/GitHub/rippling-dbt
AIRFLOW_DAGS_PATH=~/Documents/GitHub/airflow-dags

# Optional: Snowflake (SSO)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_email
SNOWFLAKE_AUTHENTICATOR=externalbrowser
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=prod_rippling_dwh

# Optional: AI (for smart grouping and explanations)
OPENAI_API_KEY=your_key
OPENAI_MODEL=o1
```

### 3. Prepare dbt

Ensure you have a compiled dbt manifest:

```bash
cd ~/Documents/GitHub/rippling-dbt
dbt compile  # Generates target/manifest.json
```

### 4. Start the app

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000)

### 5. Build the graph

Click "Build Graph" to start indexing. This will:
1. Parse your dbt manifest
2. Parse Airflow DAG files and SQL
3. (Optional) Fetch Snowflake metadata
4. Infer groups and flows
5. Generate AI explanations for key models

### 6. Explore

Once indexing completes, click "Open Graph Explorer" to:
- Navigate the interactive graph
- Click groups to collapse/expand
- Select a flow to focus on a subgraph
- Search for specific tables
- Click nodes to see details in the side panel

### 7. Reset (Start Fresh)

To clear all indexed data and start from scratch:
- Click the **Reset** button on the home page (after indexing) or in the explorer header
- Confirm the action in the dialog
- All nodes, edges, flows, groups, and AI explanations will be deleted
- You can then re-analyze your pipeline with fresh data

## Architecture

```
app/
├── page.tsx              # Home / indexing UI
├── explorer/page.tsx     # Graph explorer
├── api/
│   ├── ingest/           # Start indexing job
│   ├── status/           # Job progress
│   ├── graph/            # Get graph data
│   ├── search/           # Full-text search
│   ├── node/             # Node details
│   ├── explain/          # AI explanations
│   ├── column-lineage/   # Column-level lineage
│   └── reset/            # Clear all data

components/
├── GraphExplorer.tsx     # Cytoscape.js wrapper
├── FlowSelector.tsx      # Flow picker
├── SearchBar.tsx         # Search with autocomplete
├── DepthControl.tsx      # Neighborhood depth
├── MiniMap.tsx           # Overview map

lib/
├── db/                   # SQLite storage
├── indexer/              # Parsing & indexing
│   ├── dbtParser.ts      # dbt manifest parsing
│   ├── airflowParser.ts  # DAG & SQL parsing
│   ├── linker.ts         # Cross-repo linking
│   └── snowflakeMetadata.ts
├── ai/                   # AI integration
│   ├── client.ts         # OpenAI wrapper
│   ├── grouping.ts       # Group inference
│   ├── flows.ts          # Flow proposals
│   └── explain.ts        # Explanations
├── parsers/
│   └── sqlParser.ts      # SQL parsing
└── snowflake/
    └── client.ts         # Snowflake SDK
```

## Key Flows

### Mechanized Outreach
The primary test flow, tracing lead enrichment from:
- Sources: Apollo, ZoomInfo, Cognism (MECH_OUTREACH schema)
- Through staging and intermediate models
- To `mart_growth__lsw_lead_data`

### Bookings Pipeline
Revenue/ARR calculations through:
- `int_bookings__line_items_step1` through `step_final`
- To `mart_bookings__line_items_final`

### Sales Opportunities
Central opportunity data at `int_sales__opportunities`

## Data Storage

All data is stored locally in SQLite:
- `./data/pipeline.db`

Tables:
- `nodes`: Tables, views, models, sources
- `edges`: Dependencies and relationships
- `groups`: Inferred groupings
- `flows`: Detected data flows
- `citations`: File references
- `explanations`: AI-generated descriptions
- `jobs`: Indexing job status

## Development

```bash
# Start dev server
npm run dev

# Build for production
npm run build

# Type check
npx tsc --noEmit
```

## Configuration Options

| Variable | Description | Required |
|----------|-------------|----------|
| `RIPPLING_DBT_PATH` | Path to rippling-dbt clone | Yes |
| `AIRFLOW_DAGS_PATH` | Path to airflow-dags clone | Yes |
| `SNOWFLAKE_*` | Snowflake connection | No |
| `OPENAI_API_KEY` | For AI features | No |
| `DATABASE_PATH` | SQLite path (default: ./data/pipeline.db) | No |

## Troubleshooting

### "manifest.json not found"
Run `dbt compile` in your dbt project first.

### Snowflake connection fails
- Check your account/user settings
- SSO auth opens a browser window
- Ensure you have access to the specified database

### Graph is empty
- Check that repo paths are correct
- Verify dbt compile succeeded
- Check browser console for errors

### AI features not working
- Ensure `OPENAI_API_KEY` is set
- Check API key has access to the configured model

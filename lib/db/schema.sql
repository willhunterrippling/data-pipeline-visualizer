-- Core entities
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,           -- FQN: db.schema.table
    name TEXT NOT NULL,            -- Short name
    type TEXT NOT NULL,            -- table, view, model, source, seed, external
    subtype TEXT,                  -- dbt_model, airflow_table, snowflake_native
    group_id TEXT,                 -- FK to groups
    repo TEXT,                     -- rippling-dbt, airflow-dags, snowflake
    metadata TEXT,                 -- JSON: columns, tags, materialization, schedule
    sql_content TEXT,              -- Raw SQL file content (for dbt models)
    layout_x REAL,                 -- Pre-computed X position from dagre
    layout_y REAL,                 -- Pre-computed Y position from dagre
    layout_layer INTEGER,          -- Topological layer (depth from sources)
    semantic_layer TEXT,           -- Semantic classification: source, staging, intermediate, mart, report
    importance_score REAL,         -- Connectivity-based importance (0-1), higher = better anchor candidate
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS edges (
    id TEXT PRIMARY KEY,
    from_node TEXT NOT NULL,
    to_node TEXT NOT NULL,
    type TEXT NOT NULL,            -- ref, source, sql_dependency, dag_edge, materialization
    metadata TEXT,                 -- JSON: SQL snippet, transformation type
    FOREIGN KEY (from_node) REFERENCES nodes(id),
    FOREIGN KEY (to_node) REFERENCES nodes(id)
);

CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    parent_id TEXT,                -- For nested groups
    inference_reason TEXT,         -- Why AI created this group
    node_count INTEGER DEFAULT 0,
    collapsed_default INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS flows (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    anchor_nodes TEXT,             -- JSON: Starting table IDs
    member_nodes TEXT,             -- JSON: All nodes in flow
    user_defined INTEGER DEFAULT 0,
    inference_reason TEXT
);

CREATE TABLE IF NOT EXISTS layer_names (
    layer_number INTEGER PRIMARY KEY,  -- The topological layer number
    name TEXT NOT NULL,                -- AI-generated name like "Lead Enrichment Sources"
    description TEXT,                  -- Brief description of what's in this layer
    node_count INTEGER,                -- Number of nodes in this layer
    sample_nodes TEXT,                 -- JSON: Sample node names for context
    inference_reason TEXT              -- Why AI chose this name
);

CREATE TABLE IF NOT EXISTS anchor_candidates (
    node_id TEXT PRIMARY KEY,          -- FK to nodes
    importance_score REAL NOT NULL,    -- 0-1, higher = better anchor
    upstream_count INTEGER,            -- Number of upstream connections
    downstream_count INTEGER,          -- Number of downstream connections
    total_connections INTEGER,         -- Total connections
    reason TEXT,                       -- Why this is a good anchor
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

CREATE TABLE IF NOT EXISTS citations (
    id TEXT PRIMARY KEY,
    node_id TEXT,
    edge_id TEXT,
    file_path TEXT NOT NULL,
    start_line INTEGER,
    end_line INTEGER,
    snippet TEXT,
    FOREIGN KEY (node_id) REFERENCES nodes(id),
    FOREIGN KEY (edge_id) REFERENCES edges(id)
);

CREATE TABLE IF NOT EXISTS explanations (
    node_id TEXT PRIMARY KEY,
    summary TEXT,                  -- Plain-English explanation
    generated_at TEXT DEFAULT (datetime('now')),
    model_used TEXT,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);

-- Relational explanations: how a node relates to a specific anchor
CREATE TABLE IF NOT EXISTS relational_explanations (
    node_id TEXT NOT NULL,
    anchor_id TEXT NOT NULL,
    transformation_summary TEXT,  -- How data transforms along the path
    business_context TEXT,        -- Why this relationship matters
    full_explanation TEXT,        -- Combined AI-generated explanation
    generated_at TEXT DEFAULT (datetime('now')),
    model_used TEXT,
    PRIMARY KEY (node_id, anchor_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id),
    FOREIGN KEY (anchor_id) REFERENCES nodes(id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,          -- pending, running, completed, failed, waiting_for_input
    stage TEXT,
    stage_progress INTEGER DEFAULT 0,  -- 0-100 within current stage
    message TEXT,
    error TEXT,
    activity_log TEXT,             -- JSON array of {timestamp, message}
    usage_stats TEXT,              -- JSON: {totalInputTokens, totalOutputTokens, totalCalls, estimatedCostUsd}
    skipped_stages TEXT,           -- JSON array of stage IDs that were skipped/failed gracefully
    waiting_for TEXT,              -- What input we're waiting for: schema_selection, etc.
    waiting_data TEXT,             -- JSON: Data for the waiting UI (e.g., available schemas)
    selected_schemas TEXT,         -- JSON array of user-selected Snowflake schemas
    started_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Lineage cache for frequently-accessed anchors
CREATE TABLE IF NOT EXISTS lineage_cache (
    cache_key TEXT PRIMARY KEY,    -- Hash of (anchorId, upstreamDepth, downstreamDepth, flowId)
    anchor_id TEXT NOT NULL,       -- The anchor node ID for this cache entry
    upstream_depth INTEGER NOT NULL,
    downstream_depth INTEGER NOT NULL,
    flow_id TEXT,                  -- NULL if no flow filter
    result TEXT NOT NULL,          -- JSON: Serialized LineageResponse
    created_at TEXT DEFAULT (datetime('now')),
    access_count INTEGER DEFAULT 1,
    last_accessed TEXT DEFAULT (datetime('now'))
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(type);
CREATE INDEX IF NOT EXISTS idx_nodes_group ON nodes(group_id);
CREATE INDEX IF NOT EXISTS idx_nodes_repo ON nodes(repo);
CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_node);
CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_node);
CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(type);
CREATE INDEX IF NOT EXISTS idx_citations_node ON citations(node_id);
CREATE INDEX IF NOT EXISTS idx_citations_edge ON citations(edge_id);
CREATE INDEX IF NOT EXISTS idx_lineage_cache_anchor ON lineage_cache(anchor_id);
CREATE INDEX IF NOT EXISTS idx_lineage_cache_access ON lineage_cache(access_count DESC);

-- Full-text search for nodes (includes sql_content for code search)
CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
    id,
    name,
    type,
    metadata,
    sql_content,
    content='nodes',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS nodes_ai AFTER INSERT ON nodes BEGIN
    INSERT INTO nodes_fts(rowid, id, name, type, metadata, sql_content) 
    VALUES (new.rowid, new.id, new.name, new.type, new.metadata, new.sql_content);
END;

CREATE TRIGGER IF NOT EXISTS nodes_ad AFTER DELETE ON nodes BEGIN
    INSERT INTO nodes_fts(nodes_fts, rowid, id, name, type, metadata, sql_content) 
    VALUES ('delete', old.rowid, old.id, old.name, old.type, old.metadata, old.sql_content);
END;

CREATE TRIGGER IF NOT EXISTS nodes_au AFTER UPDATE ON nodes BEGIN
    INSERT INTO nodes_fts(nodes_fts, rowid, id, name, type, metadata, sql_content) 
    VALUES ('delete', old.rowid, old.id, old.name, old.type, old.metadata, old.sql_content);
    INSERT INTO nodes_fts(rowid, id, name, type, metadata, sql_content) 
    VALUES (new.rowid, new.id, new.name, new.type, new.metadata, new.sql_content);
END;


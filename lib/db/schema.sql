-- Core entities
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,           -- FQN: db.schema.table
    name TEXT NOT NULL,            -- Short name
    type TEXT NOT NULL,            -- table, view, model, source, seed, external
    subtype TEXT,                  -- dbt_model, airflow_table, snowflake_native
    group_id TEXT,                 -- FK to groups
    repo TEXT,                     -- rippling-dbt, airflow-dags, snowflake
    metadata TEXT,                 -- JSON: columns, tags, materialization, schedule
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

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,          -- pending, running, completed, failed
    stage TEXT,
    stage_progress INTEGER DEFAULT 0,  -- 0-100 within current stage
    message TEXT,
    error TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
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

-- Full-text search for nodes
CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
    id,
    name,
    type,
    metadata,
    content='nodes',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS nodes_ai AFTER INSERT ON nodes BEGIN
    INSERT INTO nodes_fts(rowid, id, name, type, metadata) 
    VALUES (new.rowid, new.id, new.name, new.type, new.metadata);
END;

CREATE TRIGGER IF NOT EXISTS nodes_ad AFTER DELETE ON nodes BEGIN
    INSERT INTO nodes_fts(nodes_fts, rowid, id, name, type, metadata) 
    VALUES ('delete', old.rowid, old.id, old.name, old.type, old.metadata);
END;

CREATE TRIGGER IF NOT EXISTS nodes_au AFTER UPDATE ON nodes BEGIN
    INSERT INTO nodes_fts(nodes_fts, rowid, id, name, type, metadata) 
    VALUES ('delete', old.rowid, old.id, old.name, old.type, old.metadata);
    INSERT INTO nodes_fts(rowid, id, name, type, metadata) 
    VALUES (new.rowid, new.id, new.name, new.type, new.metadata);
END;


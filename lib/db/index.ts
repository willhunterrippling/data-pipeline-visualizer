import Database from "better-sqlite3";
import { readFileSync } from "fs";
import { join } from "path";

// Use absolute path to ensure consistency across API routes
const DB_PATH = process.env.DATABASE_PATH || join(process.cwd(), "data/pipeline.db");

// Use globalThis to persist the database connection across module reloads in dev mode
const globalForDb = globalThis as unknown as { pipelineDb: Database.Database | undefined };

export function getDb(): Database.Database {
  if (!globalForDb.pipelineDb) {
    globalForDb.pipelineDb = new Database(DB_PATH);
    globalForDb.pipelineDb.pragma("journal_mode = WAL");
    globalForDb.pipelineDb.pragma("synchronous = NORMAL");
    globalForDb.pipelineDb.pragma("foreign_keys = ON");
    initSchema(globalForDb.pipelineDb);
  }
  return globalForDb.pipelineDb;
}

function initSchema(db: Database.Database) {
  const schemaPath = join(process.cwd(), "lib/db/schema.sql");
  const schema = readFileSync(schemaPath, "utf-8");
  db.exec(schema);
  
  // Migration: Add missing columns to nodes table if they don't exist
  const existingCols = db.prepare("PRAGMA table_info(nodes)").all() as Array<{name:string}>;
  const colNames = new Set(existingCols.map(c => c.name));
  
  const missingCols: Array<{name: string; type: string}> = [];
  if (!colNames.has('layout_x')) missingCols.push({ name: 'layout_x', type: 'REAL' });
  if (!colNames.has('layout_y')) missingCols.push({ name: 'layout_y', type: 'REAL' });
  if (!colNames.has('layout_layer')) missingCols.push({ name: 'layout_layer', type: 'INTEGER' });
  if (!colNames.has('semantic_layer')) missingCols.push({ name: 'semantic_layer', type: 'TEXT' });
  if (!colNames.has('importance_score')) missingCols.push({ name: 'importance_score', type: 'REAL' });
  
  for (const col of missingCols) {
    db.exec(`ALTER TABLE nodes ADD COLUMN ${col.name} ${col.type}`);
  }
  
  // Migration: Create relational_explanations table if it doesn't exist
  // This handles existing databases that were created before this table was added
  db.exec(`
    CREATE TABLE IF NOT EXISTS relational_explanations (
      node_id TEXT NOT NULL,
      anchor_id TEXT NOT NULL,
      transformation_summary TEXT,
      business_context TEXT,
      full_explanation TEXT,
      generated_at TEXT DEFAULT (datetime('now')),
      model_used TEXT,
      PRIMARY KEY (node_id, anchor_id),
      FOREIGN KEY (node_id) REFERENCES nodes(id),
      FOREIGN KEY (anchor_id) REFERENCES nodes(id)
    )
  `);
}

export function closeDb() {
  if (globalForDb.pipelineDb) {
    globalForDb.pipelineDb.close();
    globalForDb.pipelineDb = undefined;
  }
}

// Type-safe query helpers
export interface DbNode {
  id: string;
  name: string;
  type: string;
  subtype: string | null;
  group_id: string | null;
  repo: string | null;
  metadata: string | null;
  layout_x: number | null;
  layout_y: number | null;
  layout_layer: number | null;
  semantic_layer: string | null;
  importance_score: number | null;
  created_at: string;
}

export interface DbLayerName {
  layer_number: number;
  name: string;
  description: string | null;
  node_count: number | null;
  sample_nodes: string | null;
  inference_reason: string | null;
}

export interface DbAnchorCandidate {
  node_id: string;
  importance_score: number;
  upstream_count: number;
  downstream_count: number;
  total_connections: number;
  reason: string | null;
}

export interface DbEdge {
  id: string;
  from_node: string;
  to_node: string;
  type: string;
  metadata: string | null;
}

export interface DbGroup {
  id: string;
  name: string;
  description: string | null;
  parent_id: string | null;
  inference_reason: string | null;
  node_count: number;
  collapsed_default: number;
}

export interface DbFlow {
  id: string;
  name: string;
  description: string | null;
  anchor_nodes: string | null;
  member_nodes: string | null;
  user_defined: number;
  inference_reason: string | null;
}

export interface DbCitation {
  id: string;
  node_id: string | null;
  edge_id: string | null;
  file_path: string;
  start_line: number | null;
  end_line: number | null;
  snippet: string | null;
}

export interface DbExplanation {
  node_id: string;
  summary: string | null;
  generated_at: string;
  model_used: string | null;
}

export interface DbRelationalExplanation {
  node_id: string;
  anchor_id: string;
  transformation_summary: string | null;
  business_context: string | null;
  full_explanation: string | null;
  generated_at: string;
  model_used: string | null;
}

export interface ActivityLogEntry {
  timestamp: string;
  message: string;
}

export interface UsageStats {
  totalInputTokens: number;
  totalOutputTokens: number;
  totalCalls: number;
  estimatedCostUsd: number;
}

export interface DbJob {
  id: string;
  status: string;
  stage: string | null;
  stage_progress: number;
  message: string | null;
  error: string | null;
  activity_log: string | null;  // JSON array of ActivityLogEntry
  usage_stats: string | null;   // JSON: UsageStats
  started_at: string;
  updated_at: string;
}

// Node operations
export function insertNode(node: Omit<DbNode, "created_at">) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO nodes (id, name, type, subtype, group_id, repo, metadata)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  stmt.run(node.id, node.name, node.type, node.subtype, node.group_id, node.repo, node.metadata);
}

export function insertNodes(nodes: Omit<DbNode, "created_at">[]) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO nodes (id, name, type, subtype, group_id, repo, metadata)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const insertMany = db.transaction((items: typeof nodes) => {
    for (const node of items) {
      stmt.run(node.id, node.name, node.type, node.subtype, node.group_id, node.repo, node.metadata);
    }
  });
  insertMany(nodes);
}

export function getNodes(): DbNode[] {
  const db = getDb();
  return db.prepare("SELECT * FROM nodes").all() as DbNode[];
}

export function getNodeById(id: string): DbNode | undefined {
  const db = getDb();
  return db.prepare("SELECT * FROM nodes WHERE id = ?").get(id) as DbNode | undefined;
}

// Edge operations
export function insertEdge(edge: DbEdge) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO edges (id, from_node, to_node, type, metadata)
    VALUES (?, ?, ?, ?, ?)
  `);
  stmt.run(edge.id, edge.from_node, edge.to_node, edge.type, edge.metadata);
}

export function insertEdges(edges: DbEdge[]) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO edges (id, from_node, to_node, type, metadata)
    VALUES (?, ?, ?, ?, ?)
  `);
  const insertMany = db.transaction((items: typeof edges) => {
    for (const edge of items) {
      stmt.run(edge.id, edge.from_node, edge.to_node, edge.type, edge.metadata);
    }
  });
  insertMany(edges);
}

export function getEdges(): DbEdge[] {
  const db = getDb();
  return db.prepare("SELECT * FROM edges").all() as DbEdge[];
}

// Group operations
export function insertGroup(group: Omit<DbGroup, "node_count">) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO groups (id, name, description, parent_id, inference_reason, collapsed_default)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  stmt.run(group.id, group.name, group.description, group.parent_id, group.inference_reason, group.collapsed_default);
}

export function insertGroups(groups: Omit<DbGroup, "node_count">[]) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO groups (id, name, description, parent_id, inference_reason, collapsed_default)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  const insertMany = db.transaction((items: typeof groups) => {
    for (const group of items) {
      stmt.run(group.id, group.name, group.description, group.parent_id, group.inference_reason, group.collapsed_default);
    }
  });
  insertMany(groups);
}

export function getGroups(): DbGroup[] {
  const db = getDb();
  return db.prepare("SELECT * FROM groups").all() as DbGroup[];
}

export function updateGroupNodeCounts() {
  const db = getDb();
  db.exec(`
    UPDATE groups SET node_count = (
      SELECT COUNT(*) FROM nodes WHERE nodes.group_id = groups.id
    )
  `);
}

// Layout operations
export interface LayoutPosition {
  nodeId: string;
  x: number;
  y: number;
  layer: number;
}

export function updateNodeLayouts(positions: LayoutPosition[]) {
  const db = getDb();
  const stmt = db.prepare(`
    UPDATE nodes SET layout_x = ?, layout_y = ?, layout_layer = ? WHERE id = ?
  `);
  const updateMany = db.transaction((items: LayoutPosition[]) => {
    for (const pos of items) {
      stmt.run(pos.x, pos.y, pos.layer, pos.nodeId);
    }
  });
  updateMany(positions);
}

export function getNodesWithLayout(): DbNode[] {
  const db = getDb();
  return db.prepare("SELECT * FROM nodes WHERE layout_x IS NOT NULL").all() as DbNode[];
}

// Semantic layer and importance operations
export function updateNodeSemanticLayers(updates: Array<{ nodeId: string; semanticLayer: string }>) {
  const db = getDb();
  const stmt = db.prepare(`UPDATE nodes SET semantic_layer = ? WHERE id = ?`);
  const updateMany = db.transaction((items: typeof updates) => {
    for (const item of items) {
      stmt.run(item.semanticLayer, item.nodeId);
    }
  });
  updateMany(updates);
}

export function updateNodeImportanceScores(updates: Array<{ nodeId: string; importanceScore: number }>) {
  const db = getDb();
  const stmt = db.prepare(`UPDATE nodes SET importance_score = ? WHERE id = ?`);
  const updateMany = db.transaction((items: typeof updates) => {
    for (const item of items) {
      stmt.run(item.importanceScore, item.nodeId);
    }
  });
  updateMany(updates);
}

// Layer name operations
export function insertLayerNames(layerNames: DbLayerName[]) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO layer_names (layer_number, name, description, node_count, sample_nodes, inference_reason)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  const insertMany = db.transaction((items: DbLayerName[]) => {
    for (const ln of items) {
      stmt.run(ln.layer_number, ln.name, ln.description, ln.node_count, ln.sample_nodes, ln.inference_reason);
    }
  });
  insertMany(layerNames);
}

export function getLayerNames(): DbLayerName[] {
  const db = getDb();
  return db.prepare("SELECT * FROM layer_names ORDER BY layer_number").all() as DbLayerName[];
}

// Anchor candidate operations
export function insertAnchorCandidates(candidates: DbAnchorCandidate[]) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO anchor_candidates (node_id, importance_score, upstream_count, downstream_count, total_connections, reason)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  const insertMany = db.transaction((items: DbAnchorCandidate[]) => {
    for (const c of items) {
      stmt.run(c.node_id, c.importance_score, c.upstream_count, c.downstream_count, c.total_connections, c.reason);
    }
  });
  insertMany(candidates);
}

export function getAnchorCandidates(limit = 20): DbAnchorCandidate[] {
  const db = getDb();
  return db.prepare(
    "SELECT * FROM anchor_candidates ORDER BY importance_score DESC LIMIT ?"
  ).all(limit) as DbAnchorCandidate[];
}

// Flow operations
export function insertFlow(flow: DbFlow) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO flows (id, name, description, anchor_nodes, member_nodes, user_defined, inference_reason)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  stmt.run(flow.id, flow.name, flow.description, flow.anchor_nodes, flow.member_nodes, flow.user_defined, flow.inference_reason);
}

export function getFlows(): DbFlow[] {
  const db = getDb();
  return db.prepare("SELECT * FROM flows").all() as DbFlow[];
}

// Citation operations
export function insertCitation(citation: DbCitation) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO citations (id, node_id, edge_id, file_path, start_line, end_line, snippet)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  stmt.run(citation.id, citation.node_id, citation.edge_id, citation.file_path, citation.start_line, citation.end_line, citation.snippet);
}

export function insertCitations(citations: DbCitation[]) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO citations (id, node_id, edge_id, file_path, start_line, end_line, snippet)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const insertMany = db.transaction((items: typeof citations) => {
    for (const citation of items) {
      stmt.run(citation.id, citation.node_id, citation.edge_id, citation.file_path, citation.start_line, citation.end_line, citation.snippet);
    }
  });
  insertMany(citations);
}

export function getCitationsForNode(nodeId: string): DbCitation[] {
  const db = getDb();
  return db.prepare("SELECT * FROM citations WHERE node_id = ?").all(nodeId) as DbCitation[];
}

// Explanation operations
export function insertExplanation(explanation: DbExplanation) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO explanations (node_id, summary, model_used)
    VALUES (?, ?, ?)
  `);
  stmt.run(explanation.node_id, explanation.summary, explanation.model_used);
}

export function getExplanation(nodeId: string): DbExplanation | undefined {
  const db = getDb();
  return db.prepare("SELECT * FROM explanations WHERE node_id = ?").get(nodeId) as DbExplanation | undefined;
}

// Relational explanation operations
export function insertRelationalExplanation(explanation: DbRelationalExplanation) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO relational_explanations 
    (node_id, anchor_id, transformation_summary, business_context, full_explanation, model_used)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  stmt.run(
    explanation.node_id,
    explanation.anchor_id,
    explanation.transformation_summary,
    explanation.business_context,
    explanation.full_explanation,
    explanation.model_used
  );
}

export function getRelationalExplanation(nodeId: string, anchorId: string): DbRelationalExplanation | undefined {
  const db = getDb();
  return db.prepare(
    "SELECT * FROM relational_explanations WHERE node_id = ? AND anchor_id = ?"
  ).get(nodeId, anchorId) as DbRelationalExplanation | undefined;
}

// Job operations
export function createJob(id: string): DbJob {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO jobs (id, status, stage, stage_progress, message, activity_log)
    VALUES (?, 'pending', NULL, 0, NULL, '[]')
  `);
  stmt.run(id);
  return getJob(id)!;
}

export function updateJob(id: string, update: Partial<Pick<DbJob, "status" | "stage" | "stage_progress" | "message" | "error">>) {
  const db = getDb();
  const fields: string[] = ["updated_at = datetime('now')"];
  const values: (string | number | null)[] = [];
  
  if (update.status !== undefined) {
    fields.push("status = ?");
    values.push(update.status);
  }
  if (update.stage !== undefined) {
    fields.push("stage = ?");
    values.push(update.stage);
  }
  if (update.stage_progress !== undefined) {
    fields.push("stage_progress = ?");
    values.push(update.stage_progress);
  }
  if (update.message !== undefined) {
    fields.push("message = ?");
    values.push(update.message);
  }
  if (update.error !== undefined) {
    fields.push("error = ?");
    values.push(update.error);
  }
  
  values.push(id);
  const sql = `UPDATE jobs SET ${fields.join(", ")} WHERE id = ?`;
  db.prepare(sql).run(...values);
}

export function getJob(id: string): DbJob | undefined {
  const db = getDb();
  return db.prepare("SELECT * FROM jobs WHERE id = ?").get(id) as DbJob | undefined;
}

const MAX_ACTIVITY_LOG_ENTRIES = 50;

export function appendActivityLog(id: string, message: string): void {
  const db = getDb();
  const job = getJob(id);
  if (!job) return;

  const log: ActivityLogEntry[] = job.activity_log ? JSON.parse(job.activity_log) : [];
  log.push({
    timestamp: new Date().toISOString(),
    message,
  });

  // Keep only the last N entries
  const trimmedLog = log.slice(-MAX_ACTIVITY_LOG_ENTRIES);

  db.prepare("UPDATE jobs SET activity_log = ?, updated_at = datetime('now') WHERE id = ?")
    .run(JSON.stringify(trimmedLog), id);
}

export function getActivityLog(id: string): ActivityLogEntry[] {
  const job = getJob(id);
  if (!job || !job.activity_log) return [];
  return JSON.parse(job.activity_log);
}

export function updateUsageStats(id: string, stats: UsageStats): void {
  const db = getDb();
  db.prepare("UPDATE jobs SET usage_stats = ?, updated_at = datetime('now') WHERE id = ?")
    .run(JSON.stringify(stats), id);
}

export function getUsageStats(id: string): UsageStats | null {
  const job = getJob(id);
  if (!job || !job.usage_stats) return null;
  return JSON.parse(job.usage_stats);
}

// Search operations
export function searchNodes(query: string, limit = 50): DbNode[] {
  const db = getDb();
  return db.prepare(`
    SELECT nodes.* FROM nodes_fts
    JOIN nodes ON nodes_fts.id = nodes.id
    WHERE nodes_fts MATCH ?
    LIMIT ?
  `).all(query, limit) as DbNode[];
}

// Graph queries
export function getUpstreamNodes(nodeId: string, depth = 3): DbNode[] {
  const db = getDb();
  return db.prepare(`
    WITH RECURSIVE upstream AS (
      SELECT from_node as id, 1 as depth
      FROM edges WHERE to_node = ?
      UNION ALL
      SELECT e.from_node, u.depth + 1
      FROM edges e
      JOIN upstream u ON e.to_node = u.id
      WHERE u.depth < ?
    )
    SELECT DISTINCT n.* FROM upstream u
    JOIN nodes n ON u.id = n.id
  `).all(nodeId, depth) as DbNode[];
}

export function getDownstreamNodes(nodeId: string, depth = 3): DbNode[] {
  const db = getDb();
  return db.prepare(`
    WITH RECURSIVE downstream AS (
      SELECT to_node as id, 1 as depth
      FROM edges WHERE from_node = ?
      UNION ALL
      SELECT e.to_node, d.depth + 1
      FROM edges e
      JOIN downstream d ON e.from_node = d.id
      WHERE d.depth < ?
    )
    SELECT DISTINCT n.* FROM downstream d
    JOIN nodes n ON d.id = n.id
  `).all(nodeId, depth) as DbNode[];
}

// Clear all data (for re-indexing) - but keep jobs for status tracking
// By default, preserves explanations since they're expensive to regenerate
// Order matters due to foreign key constraints:
// 1. Tables that reference nodes/edges must be deleted first
// 2. Then edges (references nodes)
// 3. Then nodes (references groups)
// 4. Then groups and independent tables
export function clearAllData(options?: { preserveExplanations?: boolean }) {
  const db = getDb();
  const preserveExplanations = options?.preserveExplanations ?? true; // Default to preserving
  
  // Temporarily disable foreign key constraints to allow deletion with preserved explanations
  // (explanations reference nodes, but we want to keep them for cache)
  db.exec("PRAGMA foreign_keys = OFF");
  
  try {
    db.exec(`
      ${preserveExplanations ? '-- Preserving explanations' : 'DELETE FROM explanations;'}
      ${preserveExplanations ? '-- Preserving relational_explanations' : 'DELETE FROM relational_explanations;'}
      DELETE FROM citations;
      DELETE FROM anchor_candidates;
      DELETE FROM lineage_cache;
      DELETE FROM edges;
      DELETE FROM nodes;
      DELETE FROM groups;
      DELETE FROM flows;
      DELETE FROM layer_names;
    `);
  } finally {
    // Re-enable foreign key constraints
    db.exec("PRAGMA foreign_keys = ON");
  }
}

// Clean up orphaned explanations (explanations for nodes that no longer exist)
// Should be called after re-indexing is complete to remove stale explanations
export function cleanupOrphanedExplanations(): number {
  const db = getDb();
  const result = db.prepare(`
    DELETE FROM explanations WHERE node_id NOT IN (SELECT id FROM nodes)
  `).run();
  return result.changes;
}

// Clean up orphaned relational explanations
// Removes entries where either node_id or anchor_id no longer exists
export function cleanupOrphanedRelationalExplanations(): number {
  const db = getDb();
  const result = db.prepare(`
    DELETE FROM relational_explanations 
    WHERE node_id NOT IN (SELECT id FROM nodes) 
       OR anchor_id NOT IN (SELECT id FROM nodes)
  `).run();
  return result.changes;
}

// ============================================================================
// Lineage Cache Operations
// ============================================================================

export interface DbLineageCache {
  cache_key: string;
  anchor_id: string;
  upstream_depth: number;
  downstream_depth: number;
  flow_id: string | null;
  result: string;
  created_at: string;
  access_count: number;
  last_accessed: string;
}

const LINEAGE_CACHE_MAX_ENTRIES = 1000;

/**
 * Generate a cache key for lineage query parameters.
 */
export function generateLineageCacheKey(
  anchorId: string,
  upstreamDepth: number,
  downstreamDepth: number,
  flowId: string | null
): string {
  // Simple hash function - good enough for cache keys
  const input = `${anchorId}|${upstreamDepth}|${downstreamDepth}|${flowId || "null"}`;
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    const char = input.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return `lc_${Math.abs(hash).toString(16)}`;
}

/**
 * Get cached lineage result if available.
 * Returns null if not cached.
 */
export function getLineageCache(cacheKey: string): DbLineageCache | null {
  const db = getDb();
  const cached = db.prepare("SELECT * FROM lineage_cache WHERE cache_key = ?").get(cacheKey) as DbLineageCache | undefined;
  
  if (cached) {
    // Update access stats
    db.prepare(`
      UPDATE lineage_cache 
      SET access_count = access_count + 1, last_accessed = datetime('now') 
      WHERE cache_key = ?
    `).run(cacheKey);
    return cached;
  }
  
  return null;
}

/**
 * Store lineage result in cache.
 * Implements LRU eviction when cache is full.
 */
export function setLineageCache(
  cacheKey: string,
  anchorId: string,
  upstreamDepth: number,
  downstreamDepth: number,
  flowId: string | null,
  result: string
): void {
  const db = getDb();
  
  // Check current cache size
  const countResult = db.prepare("SELECT COUNT(*) as count FROM lineage_cache").get() as { count: number };
  
  // Evict LRU entries if at capacity
  if (countResult.count >= LINEAGE_CACHE_MAX_ENTRIES) {
    const toEvict = Math.floor(LINEAGE_CACHE_MAX_ENTRIES * 0.1); // Evict 10%
    db.prepare(`
      DELETE FROM lineage_cache 
      WHERE cache_key IN (
        SELECT cache_key FROM lineage_cache 
        ORDER BY last_accessed ASC 
        LIMIT ?
      )
    `).run(toEvict);
  }
  
  // Insert or replace cache entry
  db.prepare(`
    INSERT OR REPLACE INTO lineage_cache 
    (cache_key, anchor_id, upstream_depth, downstream_depth, flow_id, result, access_count, last_accessed)
    VALUES (?, ?, ?, ?, ?, ?, 1, datetime('now'))
  `).run(cacheKey, anchorId, upstreamDepth, downstreamDepth, flowId, result);
}

/**
 * Clear all lineage cache entries.
 * Called during re-indexing when graph structure changes.
 */
export function clearLineageCache(): void {
  const db = getDb();
  db.exec("DELETE FROM lineage_cache");
}

/**
 * Clear cache entries for a specific anchor node.
 * Useful when a specific node is updated.
 */
export function clearLineageCacheForAnchor(anchorId: string): void {
  const db = getDb();
  db.prepare("DELETE FROM lineage_cache WHERE anchor_id = ?").run(anchorId);
}

/**
 * Get cache statistics for monitoring.
 */
export function getLineageCacheStats(): {
  totalEntries: number;
  totalHits: number;
  topAnchors: Array<{ anchorId: string; hitCount: number }>;
} {
  const db = getDb();
  
  const stats = db.prepare(`
    SELECT 
      COUNT(*) as total_entries,
      SUM(access_count) as total_hits
    FROM lineage_cache
  `).get() as { total_entries: number; total_hits: number };
  
  const topAnchors = db.prepare(`
    SELECT anchor_id, SUM(access_count) as hit_count
    FROM lineage_cache
    GROUP BY anchor_id
    ORDER BY hit_count DESC
    LIMIT 10
  `).all() as Array<{ anchor_id: string; hit_count: number }>;
  
  return {
    totalEntries: stats.total_entries || 0,
    totalHits: stats.total_hits || 0,
    topAnchors: topAnchors.map(a => ({ anchorId: a.anchor_id, hitCount: a.hit_count })),
  };
}


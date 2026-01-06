import Database from "better-sqlite3";
import { readFileSync } from "fs";
import { join } from "path";

const DB_PATH = process.env.DATABASE_PATH || "./data/pipeline.db";

let db: Database.Database | null = null;

export function getDb(): Database.Database {
  if (!db) {
    db = new Database(DB_PATH);
    db.pragma("journal_mode = WAL");
    db.pragma("foreign_keys = ON");
    initSchema();
  }
  return db;
}

function initSchema() {
  const schemaPath = join(process.cwd(), "lib/db/schema.sql");
  const schema = readFileSync(schemaPath, "utf-8");
  db!.exec(schema);
}

export function closeDb() {
  if (db) {
    db.close();
    db = null;
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
  created_at: string;
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

export interface DbJob {
  id: string;
  status: string;
  stage: string | null;
  stage_progress: number;
  message: string | null;
  error: string | null;
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

// Job operations
export function createJob(id: string): DbJob {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT INTO jobs (id, status, stage, stage_progress, message)
    VALUES (?, 'pending', NULL, 0, NULL)
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

// Clear all data (for re-indexing)
export function clearAllData() {
  const db = getDb();
  db.exec(`
    DELETE FROM explanations;
    DELETE FROM citations;
    DELETE FROM flows;
    DELETE FROM groups;
    DELETE FROM edges;
    DELETE FROM nodes;
    DELETE FROM jobs;
  `);
}


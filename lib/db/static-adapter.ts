/**
 * Static data adapter for production deployment.
 * Loads graph data from JSON and provides the same interface as SQLite.
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import type {
  DbNode,
  DbEdge,
  DbGroup,
  DbFlow,
  DbCitation,
  DbExplanation,
  DbRelationalExplanation,
  DbLayerName,
  DbAnchorCandidate,
  DbJob,
  LayoutPosition,
  ActivityLogEntry,
  UsageStats,
} from "./index";

// Path to the exported JSON file
const DATA_PATH = join(process.cwd(), "public/graph-data.json");

interface ExportedData {
  nodes: DbNode[];
  edges: DbEdge[];
  groups: DbGroup[];
  flows: DbFlow[];
  explanations: DbExplanation[];
  relationalExplanations: DbRelationalExplanation[];
  layerNames: DbLayerName[];
  anchorCandidates: DbAnchorCandidate[];
  adjacency: {
    upstream: Record<string, string[]>;
    downstream: Record<string, string[]>;
  };
  exportedAt: string;
}

// In-memory data store (singleton)
let data: ExportedData | null = null;
let nodeMap: Map<string, DbNode> | null = null;
let explanationMap: Map<string, DbExplanation> | null = null;
let relationalExplanationMap: Map<string, DbRelationalExplanation> | null = null;
let flowMap: Map<string, DbFlow> | null = null;

/**
 * Load data from JSON file into memory.
 * Called lazily on first access.
 */
function loadData(): ExportedData {
  if (data) return data;

  if (!existsSync(DATA_PATH)) {
    throw new Error(
      `Static data file not found at ${DATA_PATH}. ` +
      `Run 'npm run export-graph' locally to generate it.`
    );
  }

  console.log("[static-adapter] Loading graph data from JSON...");
  const jsonString = readFileSync(DATA_PATH, "utf-8");
  data = JSON.parse(jsonString) as ExportedData;

  // Build lookup maps for fast access
  nodeMap = new Map(data.nodes.map((n) => [n.id, n]));
  explanationMap = new Map(data.explanations.map((e) => [e.node_id, e]));
  relationalExplanationMap = new Map(
    data.relationalExplanations.map((e) => [`${e.node_id}:${e.anchor_id}`, e])
  );
  flowMap = new Map(data.flows.map((f) => [f.id, f]));

  console.log(
    `[static-adapter] Loaded ${data.nodes.length} nodes, ${data.edges.length} edges, ${data.flows.length} flows`
  );
  return data;
}

// ============================================================================
// Read Operations (work in static mode)
// ============================================================================

export function getNodes(): DbNode[] {
  return loadData().nodes;
}

export function getNodeById(id: string): DbNode | undefined {
  loadData();
  return nodeMap?.get(id);
}

export function getEdges(): DbEdge[] {
  return loadData().edges;
}

export function getGroups(): DbGroup[] {
  return loadData().groups;
}

export function getFlows(): DbFlow[] {
  loadData();
  // Return from flowMap which includes both original and newly created flows
  return flowMap ? Array.from(flowMap.values()) : [];
}

export function getExplanation(nodeId: string): DbExplanation | undefined {
  loadData();
  return explanationMap?.get(nodeId);
}

export function getRelationalExplanation(
  nodeId: string,
  anchorId: string
): DbRelationalExplanation | undefined {
  loadData();
  return relationalExplanationMap?.get(`${nodeId}:${anchorId}`);
}

export function getLayerNames(): DbLayerName[] {
  return loadData().layerNames;
}

export function getAnchorCandidates(limit = 20): DbAnchorCandidate[] {
  return loadData().anchorCandidates.slice(0, limit);
}

/**
 * Search nodes by name/id using simple string matching.
 * In production, FTS is not available, so we use case-insensitive includes.
 */
export function searchNodes(query: string, limit = 50): DbNode[] {
  const data = loadData();
  const lowerQuery = query.toLowerCase().replace(/\*$/, ""); // Remove trailing * from FTS syntax

  const results: DbNode[] = [];
  for (const node of data.nodes) {
    if (results.length >= limit) break;

    const nameMatch = node.name.toLowerCase().includes(lowerQuery);
    const idMatch = node.id.toLowerCase().includes(lowerQuery);

    if (nameMatch || idMatch) {
      results.push(node);
    }
  }

  return results;
}

/**
 * Get upstream nodes using pre-computed adjacency list.
 */
export function getUpstreamNodes(nodeId: string, depth = 3): DbNode[] {
  const data = loadData();
  const visited = new Set<string>();
  const result: DbNode[] = [];

  function traverse(currentId: string, currentDepth: number) {
    if (currentDepth > depth || visited.has(currentId)) return;
    visited.add(currentId);

    const upstreamIds = data.adjacency.upstream[currentId] || [];
    for (const upId of upstreamIds) {
      if (!visited.has(upId)) {
        const node = nodeMap?.get(upId);
        if (node) {
          result.push(node);
          traverse(upId, currentDepth + 1);
        }
      }
    }
  }

  traverse(nodeId, 1);
  return result;
}

/**
 * Get downstream nodes using pre-computed adjacency list.
 */
export function getDownstreamNodes(nodeId: string, depth = 3): DbNode[] {
  const data = loadData();
  const visited = new Set<string>();
  const result: DbNode[] = [];

  function traverse(currentId: string, currentDepth: number) {
    if (currentDepth > depth || visited.has(currentId)) return;
    visited.add(currentId);

    const downstreamIds = data.adjacency.downstream[currentId] || [];
    for (const downId of downstreamIds) {
      if (!visited.has(downId)) {
        const node = nodeMap?.get(downId);
        if (node) {
          result.push(node);
          traverse(downId, currentDepth + 1);
        }
      }
    }
  }

  traverse(nodeId, 1);
  return result;
}

export function getNodesWithLayout(): DbNode[] {
  return loadData().nodes.filter((n) => n.layout_x !== null);
}

export function getCitationsForNode(_nodeId: string): DbCitation[] {
  // Citations are not exported to JSON (they contain file paths)
  return [];
}

// ============================================================================
// Lineage Cache (in-memory for static mode)
// ============================================================================

const lineageCache = new Map<string, { result: string; createdAt: string }>();

export function generateLineageCacheKey(
  anchorId: string,
  upstreamDepth: number,
  downstreamDepth: number,
  flowId: string | null
): string {
  const input = `${anchorId}|${upstreamDepth}|${downstreamDepth}|${flowId || "null"}`;
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    const char = input.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash;
  }
  return `lc_${Math.abs(hash).toString(16)}`;
}

export function getLineageCache(cacheKey: string): { result: string; created_at: string } | null {
  const cached = lineageCache.get(cacheKey);
  if (cached) {
    return { result: cached.result, created_at: cached.createdAt };
  }
  return null;
}

export function setLineageCache(
  cacheKey: string,
  _anchorId: string,
  _upstreamDepth: number,
  _downstreamDepth: number,
  _flowId: string | null,
  result: string
): void {
  // Limit cache size in memory
  if (lineageCache.size > 100) {
    const firstKey = lineageCache.keys().next().value;
    if (firstKey) lineageCache.delete(firstKey);
  }
  lineageCache.set(cacheKey, { result, createdAt: new Date().toISOString() });
}

export function clearLineageCache(): void {
  lineageCache.clear();
}

export function clearLineageCacheForAnchor(anchorId: string): void {
  // Simple implementation - clear all cache entries containing this anchor
  const keysToDelete: string[] = [];
  lineageCache.forEach((_, key) => {
    if (key.includes(anchorId)) {
      keysToDelete.push(key);
    }
  });
  keysToDelete.forEach((key) => lineageCache.delete(key));
}

export function getLineageCacheStats(): {
  totalEntries: number;
  totalHits: number;
  topAnchors: Array<{ anchorId: string; hitCount: number }>;
} {
  return {
    totalEntries: lineageCache.size,
    totalHits: 0,
    topAnchors: [],
  };
}

// ============================================================================
// Write Operations (disabled in static mode)
// ============================================================================

function throwReadOnly(): never {
  throw new Error("Write operations are not available in static mode");
}

export function getDb(): never {
  throw new Error(
    "Direct database access is not available in static mode. " +
    "Use the exported functions instead."
  );
}

export function closeDb(): void {
  // No-op in static mode
}

export function insertNode(_node: Omit<DbNode, "created_at">): never {
  throwReadOnly();
}

export function insertNodes(_nodes: Omit<DbNode, "created_at">[]): never {
  throwReadOnly();
}

export function insertEdge(_edge: DbEdge): never {
  throwReadOnly();
}

export function insertEdges(_edges: DbEdge[]): never {
  throwReadOnly();
}

export function insertGroup(_group: Omit<DbGroup, "node_count">): never {
  throwReadOnly();
}

export function insertGroups(_groups: Omit<DbGroup, "node_count">[]): never {
  throwReadOnly();
}

export function updateGroupNodeCounts(): never {
  throwReadOnly();
}

export function updateNodeLayouts(_positions: LayoutPosition[]): never {
  throwReadOnly();
}

export function updateNodeSemanticLayers(_updates: Array<{ nodeId: string; semanticLayer: string }>): never {
  throwReadOnly();
}

export function updateNodeImportanceScores(_updates: Array<{ nodeId: string; importanceScore: number }>): never {
  throwReadOnly();
}

export function insertLayerNames(_layerNames: DbLayerName[]): never {
  throwReadOnly();
}

export function insertAnchorCandidates(_candidates: DbAnchorCandidate[]): never {
  throwReadOnly();
}

export function insertFlow(flow: DbFlow): void {
  // Allow creating/updating flows in memory
  loadData();
  flowMap?.set(flow.id, flow);
}

export function insertCitation(_citation: DbCitation): never {
  throwReadOnly();
}

export function insertCitations(_citations: DbCitation[]): never {
  throwReadOnly();
}

export function insertExplanation(explanation: DbExplanation): void {
  // Allow storing explanations in memory (for AI-generated ones)
  loadData();
  explanationMap?.set(explanation.node_id, explanation);
}

export function insertRelationalExplanation(explanation: DbRelationalExplanation): void {
  // Allow storing relational explanations in memory (for AI-generated ones)
  loadData();
  relationalExplanationMap?.set(`${explanation.node_id}:${explanation.anchor_id}`, explanation);
}

export function createJob(_id: string): DbJob {
  throw new Error("Job creation is not available in static mode");
}

export function updateJob(
  _id: string,
  _update: Partial<Pick<DbJob, "status" | "stage" | "stage_progress" | "message" | "error">>
): void {
  // No-op in static mode
}

export function getJob(_id: string): DbJob | undefined {
  return undefined;
}

export function appendActivityLog(_id: string, _message: string): void {
  // No-op
}

export function getActivityLog(_id: string): ActivityLogEntry[] {
  return [];
}

export function updateUsageStats(_id: string, _stats: UsageStats): void {
  // No-op
}

export function getUsageStats(_id: string): UsageStats | null {
  return null;
}

export function markStageSkipped(_id: string, _stageId: string): void {
  // No-op in static mode
}

export function getSkippedStages(_id: string): string[] {
  return [];
}

export function setJobWaitingForSchemas(_id: string, _schemas: string[]): void {
  // No-op in static mode
}

export function submitSchemaSelection(_id: string, _selectedSchemas: string[]): void {
  // No-op in static mode
}

export function getSelectedSchemas(_id: string): string[] | null {
  return null;
}

export function getWaitingData(_id: string): { waitingFor: string; data: unknown } | null {
  return null;
}

export function clearAllData(_options?: { preserveExplanations?: boolean }): never {
  throwReadOnly();
}

export function cleanupOrphanedExplanations(): number {
  return 0;
}

export function cleanupOrphanedRelationalExplanations(): number {
  return 0;
}

// ============================================================================
// Static mode detection
// ============================================================================

export function isStaticMode(): boolean {
  return true;
}

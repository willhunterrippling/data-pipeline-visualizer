// ============================================================================
// Environment Detection - Use static adapter on Vercel, SQLite locally
// ============================================================================

const USE_STATIC_MODE =
  process.env.USE_STATIC_DATA === "true" ||
  process.env.VERCEL === "1" ||
  process.env.NEXT_RUNTIME === "edge";

// Import both implementations
import * as staticAdapter from "./static-adapter";

// Define a common adapter interface type
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type DbAdapter = any;

// Only import SQLite adapter in non-static mode to avoid bundling issues
let sqliteAdapter: DbAdapter | null = null;

// Type-safe query helpers (shared between both adapters)
export interface DbNode {
  id: string;
  name: string;
  type: string;
  subtype: string | null;
  group_id: string | null;
  repo: string | null;
  metadata: string | null;
  sql_content: string | null;
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
  activity_log: string | null;
  usage_stats: string | null;
  started_at: string;
  updated_at: string;
}

export interface LayoutPosition {
  nodeId: string;
  x: number;
  y: number;
  layer: number;
}

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

// ============================================================================
// Lazy-load SQLite adapter only when needed (not in static mode)
// ============================================================================

function getSqliteAdapterSync(): DbAdapter {
  if (USE_STATIC_MODE) {
    throw new Error("SQLite adapter is not available in static mode");
  }
  if (!sqliteAdapter) {
    // Synchronous require for initial load
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    sqliteAdapter = require("./sqlite");
  }
  return sqliteAdapter!;
}

// ============================================================================
// Exported Functions - Delegate to appropriate adapter
// ============================================================================

export function isStaticMode(): boolean {
  return USE_STATIC_MODE;
}

export function getDb() {
  if (USE_STATIC_MODE) {
    return staticAdapter.getDb();
  }
  return getSqliteAdapterSync().getDb();
}

export function closeDb(): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.closeDb();
  }
  return getSqliteAdapterSync().closeDb();
}

// Node operations
export function getNodes(): DbNode[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getNodes();
  }
  return getSqliteAdapterSync().getNodes();
}

export function getNodeById(id: string): DbNode | undefined {
  if (USE_STATIC_MODE) {
    return staticAdapter.getNodeById(id);
  }
  return getSqliteAdapterSync().getNodeById(id);
}

export function insertNode(node: Omit<DbNode, "created_at">): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertNode(node);
  }
  return getSqliteAdapterSync().insertNode(node);
}

export function insertNodes(nodes: Omit<DbNode, "created_at">[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertNodes(nodes);
  }
  return getSqliteAdapterSync().insertNodes(nodes);
}

// Edge operations
export function getEdges(): DbEdge[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getEdges();
  }
  return getSqliteAdapterSync().getEdges();
}

export function insertEdge(edge: DbEdge): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertEdge(edge);
  }
  return getSqliteAdapterSync().insertEdge(edge);
}

export function insertEdges(edges: DbEdge[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertEdges(edges);
  }
  return getSqliteAdapterSync().insertEdges(edges);
}

// Group operations
export function getGroups(): DbGroup[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getGroups();
  }
  return getSqliteAdapterSync().getGroups();
}

export function insertGroup(group: Omit<DbGroup, "node_count">): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertGroup(group);
  }
  return getSqliteAdapterSync().insertGroup(group);
}

export function insertGroups(groups: Omit<DbGroup, "node_count">[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertGroups(groups);
  }
  return getSqliteAdapterSync().insertGroups(groups);
}

export function updateGroupNodeCounts(): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.updateGroupNodeCounts();
  }
  return getSqliteAdapterSync().updateGroupNodeCounts();
}

// Layout operations
export function updateNodeLayouts(positions: LayoutPosition[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.updateNodeLayouts(positions);
  }
  return getSqliteAdapterSync().updateNodeLayouts(positions);
}

export function getNodesWithLayout(): DbNode[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getNodesWithLayout();
  }
  return getSqliteAdapterSync().getNodesWithLayout();
}

// Semantic layer and importance operations
export function updateNodeSemanticLayers(updates: Array<{ nodeId: string; semanticLayer: string }>): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.updateNodeSemanticLayers(updates);
  }
  return getSqliteAdapterSync().updateNodeSemanticLayers(updates);
}

export function updateNodeImportanceScores(updates: Array<{ nodeId: string; importanceScore: number }>): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.updateNodeImportanceScores(updates);
  }
  return getSqliteAdapterSync().updateNodeImportanceScores(updates);
}

// Layer name operations
export function insertLayerNames(layerNames: DbLayerName[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertLayerNames(layerNames);
  }
  return getSqliteAdapterSync().insertLayerNames(layerNames);
}

export function getLayerNames(): DbLayerName[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getLayerNames();
  }
  return getSqliteAdapterSync().getLayerNames();
}

// Anchor candidate operations
export function insertAnchorCandidates(candidates: DbAnchorCandidate[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertAnchorCandidates(candidates);
  }
  return getSqliteAdapterSync().insertAnchorCandidates(candidates);
}

export function getAnchorCandidates(limit = 20): DbAnchorCandidate[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getAnchorCandidates(limit);
  }
  return getSqliteAdapterSync().getAnchorCandidates(limit);
}

// Flow operations
export function insertFlow(flow: DbFlow): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertFlow(flow);
  }
  return getSqliteAdapterSync().insertFlow(flow);
}

export function getFlows(): DbFlow[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getFlows();
  }
  return getSqliteAdapterSync().getFlows();
}

// Citation operations
export function insertCitation(citation: DbCitation): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertCitation(citation);
  }
  return getSqliteAdapterSync().insertCitation(citation);
}

export function insertCitations(citations: DbCitation[]): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertCitations(citations);
  }
  return getSqliteAdapterSync().insertCitations(citations);
}

export function getCitationsForNode(nodeId: string): DbCitation[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getCitationsForNode(nodeId);
  }
  return getSqliteAdapterSync().getCitationsForNode(nodeId);
}

// Explanation operations
export function insertExplanation(explanation: DbExplanation): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertExplanation(explanation);
  }
  return getSqliteAdapterSync().insertExplanation(explanation);
}

export function getExplanation(nodeId: string): DbExplanation | undefined {
  if (USE_STATIC_MODE) {
    return staticAdapter.getExplanation(nodeId);
  }
  return getSqliteAdapterSync().getExplanation(nodeId);
}

// Relational explanation operations
export function insertRelationalExplanation(explanation: DbRelationalExplanation): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.insertRelationalExplanation(explanation);
  }
  return getSqliteAdapterSync().insertRelationalExplanation(explanation);
}

export function getRelationalExplanation(nodeId: string, anchorId: string): DbRelationalExplanation | undefined {
  if (USE_STATIC_MODE) {
    return staticAdapter.getRelationalExplanation(nodeId, anchorId);
  }
  return getSqliteAdapterSync().getRelationalExplanation(nodeId, anchorId);
}

// Job operations
export function createJob(id: string): DbJob {
  if (USE_STATIC_MODE) {
    return staticAdapter.createJob(id);
  }
  return getSqliteAdapterSync().createJob(id);
}

export function updateJob(id: string, update: Partial<Pick<DbJob, "status" | "stage" | "stage_progress" | "message" | "error">>): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.updateJob(id, update);
  }
  return getSqliteAdapterSync().updateJob(id, update);
}

export function getJob(id: string): DbJob | undefined {
  if (USE_STATIC_MODE) {
    return staticAdapter.getJob(id);
  }
  return getSqliteAdapterSync().getJob(id);
}

export function appendActivityLog(id: string, message: string): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.appendActivityLog(id, message);
  }
  return getSqliteAdapterSync().appendActivityLog(id, message);
}

export function getActivityLog(id: string): ActivityLogEntry[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getActivityLog(id);
  }
  return getSqliteAdapterSync().getActivityLog(id);
}

export function updateUsageStats(id: string, stats: UsageStats): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.updateUsageStats(id, stats);
  }
  return getSqliteAdapterSync().updateUsageStats(id, stats);
}

export function getUsageStats(id: string): UsageStats | null {
  if (USE_STATIC_MODE) {
    return staticAdapter.getUsageStats(id);
  }
  return getSqliteAdapterSync().getUsageStats(id);
}

// Search operations
export function searchNodes(query: string, limit = 50): DbNode[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.searchNodes(query, limit);
  }
  return getSqliteAdapterSync().searchNodes(query, limit);
}

// Graph queries
export function getUpstreamNodes(nodeId: string, depth = 3): DbNode[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getUpstreamNodes(nodeId, depth);
  }
  return getSqliteAdapterSync().getUpstreamNodes(nodeId, depth);
}

export function getDownstreamNodes(nodeId: string, depth = 3): DbNode[] {
  if (USE_STATIC_MODE) {
    return staticAdapter.getDownstreamNodes(nodeId, depth);
  }
  return getSqliteAdapterSync().getDownstreamNodes(nodeId, depth);
}

// Clear operations
export function clearAllData(options?: { preserveExplanations?: boolean }): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.clearAllData(options);
  }
  return getSqliteAdapterSync().clearAllData(options);
}

export function cleanupOrphanedExplanations(): number {
  if (USE_STATIC_MODE) {
    return staticAdapter.cleanupOrphanedExplanations();
  }
  return getSqliteAdapterSync().cleanupOrphanedExplanations();
}

export function cleanupOrphanedRelationalExplanations(): number {
  if (USE_STATIC_MODE) {
    return staticAdapter.cleanupOrphanedRelationalExplanations();
  }
  return getSqliteAdapterSync().cleanupOrphanedRelationalExplanations();
}

// Lineage cache operations
export function generateLineageCacheKey(
  anchorId: string,
  upstreamDepth: number,
  downstreamDepth: number,
  flowId: string | null
): string {
  if (USE_STATIC_MODE) {
    return staticAdapter.generateLineageCacheKey(anchorId, upstreamDepth, downstreamDepth, flowId);
  }
  return getSqliteAdapterSync().generateLineageCacheKey(anchorId, upstreamDepth, downstreamDepth, flowId);
}

export function getLineageCache(cacheKey: string): DbLineageCache | { result: string; created_at: string } | null {
  if (USE_STATIC_MODE) {
    return staticAdapter.getLineageCache(cacheKey);
  }
  return getSqliteAdapterSync().getLineageCache(cacheKey);
}

export function setLineageCache(
  cacheKey: string,
  anchorId: string,
  upstreamDepth: number,
  downstreamDepth: number,
  flowId: string | null,
  result: string
): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.setLineageCache(cacheKey, anchorId, upstreamDepth, downstreamDepth, flowId, result);
  }
  return getSqliteAdapterSync().setLineageCache(cacheKey, anchorId, upstreamDepth, downstreamDepth, flowId, result);
}

export function clearLineageCache(): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.clearLineageCache();
  }
  return getSqliteAdapterSync().clearLineageCache();
}

export function clearLineageCacheForAnchor(anchorId: string): void {
  if (USE_STATIC_MODE) {
    return staticAdapter.clearLineageCacheForAnchor(anchorId);
  }
  return getSqliteAdapterSync().clearLineageCacheForAnchor(anchorId);
}

export function getLineageCacheStats(): {
  totalEntries: number;
  totalHits: number;
  topAnchors: Array<{ anchorId: string; hitCount: number }>;
} {
  if (USE_STATIC_MODE) {
    return staticAdapter.getLineageCacheStats();
  }
  return getSqliteAdapterSync().getLineageCacheStats();
}

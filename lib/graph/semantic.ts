/**
 * Semantic Layer Classification and Importance Scoring
 * 
 * Classifies nodes into semantic layers (source, staging, intermediate, mart, report)
 * and computes importance scores based on connectivity for anchor suggestions.
 */

import type { GraphNode, GraphEdge, SemanticLayer } from "../types";

// ============================================================================
// Semantic Layer Classification
// ============================================================================

/**
 * Classify a node into a semantic layer based on naming patterns and type.
 */
export function classifySemanticLayer(node: GraphNode): SemanticLayer {
  const name = node.name.toLowerCase();

  // External sources
  if (node.type === "external") return "external";

  // Raw sources and seeds
  if (node.type === "source" || node.type === "seed") return "source";

  // Staging models (stg_ prefix)
  if (name.startsWith("stg_")) return "staging";

  // Intermediate models (int_ prefix)
  if (name.startsWith("int_")) return "intermediate";

  // Mart/final models (mart_ prefix or _final suffix)
  if (name.startsWith("mart_") || name.endsWith("_final")) return "mart";

  // Report models (rpt_ prefix)
  if (name.startsWith("rpt_") || name.startsWith("report_")) return "report";

  // Fallback: use type to infer
  if (node.type === "view") return "report";
  if (node.type === "table") {
    // Tables with all caps are usually sources
    if (node.name === node.name.toUpperCase()) return "source";
  }

  // Default to transform for anything else
  return "transform";
}

/**
 * Classify all nodes and return updates for the database.
 */
export function classifyAllNodes(
  nodes: GraphNode[]
): Array<{ nodeId: string; semanticLayer: SemanticLayer }> {
  return nodes.map((node) => ({
    nodeId: node.id,
    semanticLayer: classifySemanticLayer(node),
  }));
}

// ============================================================================
// Importance Scoring (Connectivity-Based)
// ============================================================================

export interface NodeConnectivity {
  nodeId: string;
  upstreamCount: number;
  downstreamCount: number;
  totalConnections: number;
}

/**
 * Compute connectivity for all nodes.
 */
export function computeConnectivity(
  nodes: GraphNode[],
  edges: GraphEdge[]
): Map<string, NodeConnectivity> {
  const connectivity = new Map<string, NodeConnectivity>();

  // Initialize all nodes
  for (const node of nodes) {
    connectivity.set(node.id, {
      nodeId: node.id,
      upstreamCount: 0,
      downstreamCount: 0,
      totalConnections: 0,
    });
  }

  // Count edges
  for (const edge of edges) {
    const fromConn = connectivity.get(edge.from);
    const toConn = connectivity.get(edge.to);

    if (fromConn) {
      fromConn.downstreamCount++;
      fromConn.totalConnections++;
    }
    if (toConn) {
      toConn.upstreamCount++;
      toConn.totalConnections++;
    }
  }

  return connectivity;
}

/**
 * Compute importance scores based on connectivity.
 * Higher connectivity = more important as an anchor.
 * 
 * Score formula:
 * - Base score from total connections (normalized)
 * - Bonus for being a "hub" (both high upstream and downstream)
 * - Bonus for mart/report nodes (likely end-user tables)
 */
export function computeImportanceScores(
  nodes: GraphNode[],
  edges: GraphEdge[]
): Array<{
  nodeId: string;
  importanceScore: number;
  upstreamCount: number;
  downstreamCount: number;
  totalConnections: number;
  reason: string;
}> {
  const connectivity = computeConnectivity(nodes, edges);
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));

  // Find max connections for normalization
  let maxConnections = 1;
  for (const conn of connectivity.values()) {
    maxConnections = Math.max(maxConnections, conn.totalConnections);
  }

  const results: Array<{
    nodeId: string;
    importanceScore: number;
    upstreamCount: number;
    downstreamCount: number;
    totalConnections: number;
    reason: string;
  }> = [];

  for (const conn of connectivity.values()) {
    const node = nodeMap.get(conn.nodeId);
    if (!conn || !node) continue;

    // Base score: normalized total connections (0-0.5)
    const baseScore = (conn.totalConnections / maxConnections) * 0.5;

    // Hub bonus: high both upstream and downstream (0-0.25)
    const minDirection = Math.min(conn.upstreamCount, conn.downstreamCount);
    const hubBonus = (minDirection / maxConnections) * 0.25;

    // Semantic bonus: marts and reports are more important (0-0.25)
    const semanticLayer = classifySemanticLayer(node);
    let semanticBonus = 0;
    if (semanticLayer === "mart") semanticBonus = 0.25;
    else if (semanticLayer === "report") semanticBonus = 0.2;
    else if (semanticLayer === "intermediate") semanticBonus = 0.1;

    const importanceScore = Math.min(1, baseScore + hubBonus + semanticBonus);

    // Generate reason
    const reasons: string[] = [];
    if (conn.totalConnections > maxConnections * 0.5) {
      reasons.push("High connectivity");
    }
    if (minDirection > 3) {
      reasons.push("Hub node");
    }
    if (semanticLayer === "mart" || semanticLayer === "report") {
      reasons.push(`${semanticLayer.charAt(0).toUpperCase() + semanticLayer.slice(1)} table`);
    }

    results.push({
      nodeId: conn.nodeId,
      importanceScore,
      upstreamCount: conn.upstreamCount,
      downstreamCount: conn.downstreamCount,
      totalConnections: conn.totalConnections,
      reason: reasons.length > 0 ? reasons.join(", ") : "Standard node",
    });
  }

  return results;
}

/**
 * Get top anchor candidates (nodes with highest importance scores).
 */
export function getTopAnchorCandidates(
  nodes: GraphNode[],
  edges: GraphEdge[],
  limit = 50
): Array<{
  nodeId: string;
  importanceScore: number;
  upstreamCount: number;
  downstreamCount: number;
  totalConnections: number;
  reason: string;
}> {
  const scores = computeImportanceScores(nodes, edges);
  return scores
    .sort((a, b) => b.importanceScore - a.importanceScore)
    .slice(0, limit);
}

// ============================================================================
// Layer Statistics
// ============================================================================

export interface LayerStats {
  layerNumber: number;
  nodeCount: number;
  sampleNodes: string[];
  dominantSemanticLayer: SemanticLayer;
  prefixes: Map<string, number>;
}

/**
 * Compute statistics for each topological layer.
 */
export function computeLayerStats(nodes: GraphNode[]): Map<number, LayerStats> {
  const layerStats = new Map<number, LayerStats>();

  for (const node of nodes) {
    const layer = node.layoutLayer ?? 0;

    if (!layerStats.has(layer)) {
      layerStats.set(layer, {
        layerNumber: layer,
        nodeCount: 0,
        sampleNodes: [],
        dominantSemanticLayer: "transform",
        prefixes: new Map(),
      });
    }

    const stats = layerStats.get(layer)!;
    stats.nodeCount++;

    // Collect sample nodes
    if (stats.sampleNodes.length < 5) {
      stats.sampleNodes.push(node.name);
    }

    // Count prefixes
    const prefix = node.name.match(/^([a-z]+_)/)?.[1] || "other";
    stats.prefixes.set(prefix, (stats.prefixes.get(prefix) || 0) + 1);
  }

  // Determine dominant semantic layer for each layer
  for (const stats of layerStats.values()) {
    let maxCount = 0;
    let dominant: SemanticLayer = "transform";

    // Map prefixes to semantic layers
    const semanticCounts = new Map<SemanticLayer, number>();
    for (const [prefix, count] of stats.prefixes) {
      let semantic: SemanticLayer = "transform";
      if (prefix === "stg_") semantic = "staging";
      else if (prefix === "int_") semantic = "intermediate";
      else if (prefix === "mart_") semantic = "mart";
      else if (prefix === "rpt_" || prefix === "report_") semantic = "report";

      semanticCounts.set(semantic, (semanticCounts.get(semantic) || 0) + count);
    }

    for (const [semantic, count] of semanticCounts) {
      if (count > maxCount) {
        maxCount = count;
        dominant = semantic;
      }
    }

    stats.dominantSemanticLayer = dominant;
  }

  return layerStats;
}


/**
 * Pre-compute graph layout for stable, hierarchical positioning.
 * Layout is computed once during indexing and cached in the database.
 * This ensures layout stability - no re-layouts on visibility changes.
 * 
 * Uses a fast layer-based layout to avoid blocking the event loop.
 * For large graphs (5000+ nodes), dagre can take minutes.
 */

export interface NodePosition {
  x: number;
  y: number;
  layer: number;
}

export interface LayoutResult {
  positions: Map<string, NodePosition>;
  layers: Map<string, number>;
  bounds: { minX: number; maxX: number; minY: number; maxY: number };
}

export interface LayoutNode {
  id: string;
  name: string;
}

export interface LayoutEdge {
  from: string;
  to: string;
}

// Layout configuration
const LAYOUT_CONFIG = {
  rankDir: "LR" as const,      // Left-to-right flow
  nodeSep: 60,                  // Vertical spacing between nodes
  rankSep: 200,                 // Horizontal spacing between layers
  marginX: 50,
  marginY: 50,
  nodeWidth: 150,
  nodeHeight: 50,
};

/**
 * Compute topological layers for all nodes using BFS from sources.
 * Sources (nodes with no incoming edges) are layer 0.
 * Each subsequent layer is +1 from its maximum upstream layer.
 */
function computeTopologicalLayers(
  nodes: LayoutNode[],
  edges: LayoutEdge[]
): Map<string, number> {
  const nodeIds = new Set(nodes.map((n) => n.id));
  const layers = new Map<string, number>();

  // Build adjacency maps
  const incomingEdges = new Map<string, Set<string>>();
  const outgoingEdges = new Map<string, Set<string>>();

  for (const node of nodes) {
    incomingEdges.set(node.id, new Set());
    outgoingEdges.set(node.id, new Set());
  }

  for (const edge of edges) {
    // Only consider edges between valid nodes
    if (!nodeIds.has(edge.from) || !nodeIds.has(edge.to)) continue;
    
    incomingEdges.get(edge.to)?.add(edge.from);
    outgoingEdges.get(edge.from)?.add(edge.to);
  }

  // Find sources (nodes with no incoming edges)
  const sources: string[] = [];
  for (const node of nodes) {
    const incoming = incomingEdges.get(node.id);
    if (!incoming || incoming.size === 0) {
      sources.push(node.id);
      layers.set(node.id, 0);
    }
  }

  // BFS to assign layers
  const queue = [...sources];
  const visited = new Set<string>(sources);

  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    const currentLayer = layers.get(nodeId) ?? 0;

    const outgoing = outgoingEdges.get(nodeId) ?? new Set();
    for (const downstream of outgoing) {
      // Layer is max of all upstream layers + 1
      const existingLayer = layers.get(downstream);
      const newLayer = currentLayer + 1;

      if (existingLayer === undefined || newLayer > existingLayer) {
        layers.set(downstream, newLayer);
      }

      if (!visited.has(downstream)) {
        visited.add(downstream);
        queue.push(downstream);
      }
    }
  }

  // Handle disconnected nodes (no edges) - assign layer 0
  for (const node of nodes) {
    if (!layers.has(node.id)) {
      layers.set(node.id, 0);
    }
  }

  return layers;
}

/**
 * Pre-compute layout for the entire graph.
 * Uses a fast layer-based layout instead of dagre to avoid blocking the event loop.
 * For large graphs (5000+ nodes), dagre can take minutes and block all HTTP requests.
 * This simple layout is instant and produces clean hierarchical results.
 */
export function precomputeLayout(
  nodes: LayoutNode[],
  edges: LayoutEdge[]
): LayoutResult {
  // First compute topological layers
  const layers = computeTopologicalLayers(nodes, edges);

  // Group nodes by layer for Y positioning
  const nodesByLayer = new Map<number, LayoutNode[]>();
  for (const node of nodes) {
    const layer = layers.get(node.id) ?? 0;
    if (!nodesByLayer.has(layer)) {
      nodesByLayer.set(layer, []);
    }
    nodesByLayer.get(layer)!.push(node);
  }

  // Sort layers by layer number
  const sortedLayers = [...nodesByLayer.keys()].sort((a, b) => a - b);

  // Position nodes: X based on layer, Y based on position within layer
  const positions = new Map<string, NodePosition>();
  let minX = Infinity, maxX = -Infinity;
  let minY = Infinity, maxY = -Infinity;

  for (const layerNum of sortedLayers) {
    const layerNodes = nodesByLayer.get(layerNum)!;
    // Sort nodes within layer alphabetically for consistent ordering
    layerNodes.sort((a, b) => a.name.localeCompare(b.name));

    const x = LAYOUT_CONFIG.marginX + layerNum * LAYOUT_CONFIG.rankSep;

    for (let i = 0; i < layerNodes.length; i++) {
      const node = layerNodes[i];
      const y = LAYOUT_CONFIG.marginY + i * (LAYOUT_CONFIG.nodeHeight + LAYOUT_CONFIG.nodeSep);

      positions.set(node.id, { x, y, layer: layerNum });

      minX = Math.min(minX, x);
      maxX = Math.max(maxX, x);
      minY = Math.min(minY, y);
      maxY = Math.max(maxY, y);
    }
  }

  return {
    positions,
    layers,
    bounds: {
      minX: minX === Infinity ? 0 : minX,
      maxX: maxX === -Infinity ? 0 : maxX,
      minY: minY === Infinity ? 0 : minY,
      maxY: maxY === -Infinity ? 0 : maxY,
    },
  };
}

/**
 * Get layer name based on layer number and node characteristics.
 * Used for swimlane column headers.
 */
export function getLayerName(layer: number, maxLayer: number): string {
  if (layer === 0) return "Sources";
  if (layer === maxLayer) return "Outputs";
  
  // Estimate based on relative position
  const ratio = layer / maxLayer;
  if (ratio < 0.25) return "Staging";
  if (ratio < 0.5) return "Intermediate";
  if (ratio < 0.75) return "Marts";
  return "Downstream";
}

/**
 * Get layer name relative to an anchor node.
 * Negative layers are upstream, positive are downstream.
 */
export function getRelativeLayerName(relativeLayer: number): string {
  if (relativeLayer === 0) return "Selected";
  if (relativeLayer < -2) return "Sources";
  if (relativeLayer === -2) return "Staging";
  if (relativeLayer === -1) return "Intermediate";
  if (relativeLayer === 1) return "Consumers";
  return "Downstream";
}

// ============================================================================
// Smart Layer Naming
// ============================================================================

export interface SmartLayerName {
  layer: number;
  name: string;           // e.g., "MECH_OUTREACH Sources"
  genericName: string;    // Fallback generic name
  nodeCount: number;
  topPrefixes: string[];  // e.g., ["mech_outreach", "sfdc"]
}

/**
 * Extract the most meaningful prefix from a node name.
 * Tries to identify domain/source prefixes like "sfdc_", "mech_outreach_", etc.
 */
function extractPrefix(name: string): string | null {
  // Common dbt prefixes to skip (too generic)
  const genericPrefixes = new Set(["stg", "int", "mart", "rpt", "dim", "fct", "raw"]);
  
  // Try to extract first meaningful segment(s)
  const parts = name.toLowerCase().split("_");
  if (parts.length < 2) return null;
  
  // Skip generic prefixes and take the first meaningful one
  let startIdx = 0;
  if (genericPrefixes.has(parts[0])) {
    startIdx = 1;
  }
  
  if (startIdx >= parts.length) return null;
  
  // Take up to 2 segments for the prefix
  const prefix = parts.slice(startIdx, Math.min(startIdx + 2, parts.length - 1)).join("_");
  if (prefix.length < 2) return null;
  
  return prefix;
}

/**
 * Format a prefix into a human-readable label.
 * e.g., "mech_outreach" -> "MECH_OUTREACH", "sfdc" -> "SFDC"
 */
function formatPrefixLabel(prefix: string): string {
  // Known acronyms and special names
  const knownNames: Record<string, string> = {
    sfdc: "SFDC",
    g2: "G2",
    gartner: "Gartner",
    mongo: "MongoDB",
    postgres: "PostgreSQL",
    outreach: "Outreach",
    hubspot: "HubSpot",
    stripe: "Stripe",
    segment: "Segment",
    fivetran: "Fivetran",
    dbt: "dbt",
  };
  
  const parts = prefix.split("_");
  const formatted = parts.map(part => {
    if (knownNames[part]) return knownNames[part];
    // Uppercase short segments (likely acronyms), title case others
    if (part.length <= 3) return part.toUpperCase();
    return part.charAt(0).toUpperCase() + part.slice(1);
  });
  
  return formatted.join(" ");
}

/**
 * Compute smart layer names based on the actual node prefixes in each layer.
 * Analyzes naming patterns to create contextual names like "SFDC Sources" or "G2/Gartner Intent".
 */
export function computeSmartLayerNames(
  nodes: Array<{ id: string; name: string; relativeLayer: number }>,
  maxLayer: number
): Map<number, SmartLayerName> {
  // Group nodes by relative layer
  const nodesByLayer = new Map<number, Array<{ id: string; name: string }>>();
  
  for (const node of nodes) {
    const layer = node.relativeLayer;
    if (!nodesByLayer.has(layer)) {
      nodesByLayer.set(layer, []);
    }
    nodesByLayer.get(layer)!.push(node);
  }
  
  const result = new Map<number, SmartLayerName>();
  
  for (const [layer, layerNodes] of nodesByLayer) {
    // Count prefix occurrences
    const prefixCounts = new Map<string, number>();
    
    for (const node of layerNodes) {
      const prefix = extractPrefix(node.name);
      if (prefix) {
        prefixCounts.set(prefix, (prefixCounts.get(prefix) || 0) + 1);
      }
    }
    
    // Sort by frequency and get top prefixes
    const sortedPrefixes = [...prefixCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3);
    
    // Generate generic layer name based on position
    const genericName = getRelativeLayerName(layer);
    
    // Build smart name
    let smartName: string;
    const topPrefixes = sortedPrefixes.map(([prefix]) => prefix);
    
    if (sortedPrefixes.length === 0) {
      // No prefixes found, use generic name
      smartName = genericName;
    } else if (sortedPrefixes.length === 1 && sortedPrefixes[0][1] >= layerNodes.length * 0.5) {
      // Single dominant prefix (>50% of nodes)
      smartName = `${formatPrefixLabel(sortedPrefixes[0][0])} ${genericName}`;
    } else if (sortedPrefixes.length >= 2) {
      // Multiple prefixes - show top 2
      const top2Labels = sortedPrefixes
        .slice(0, 2)
        .map(([prefix]) => formatPrefixLabel(prefix));
      
      // Check if these prefixes cover a significant portion
      const coverage = sortedPrefixes.slice(0, 2).reduce((sum, [, count]) => sum + count, 0) / layerNodes.length;
      
      if (coverage >= 0.4) {
        smartName = `${top2Labels.join("/")} ${genericName}`;
      } else {
        // Too fragmented, use generic with hint
        smartName = `${genericName} (${top2Labels[0]}+)`;
      }
    } else {
      smartName = genericName;
    }
    
    result.set(layer, {
      layer,
      name: smartName,
      genericName,
      nodeCount: layerNodes.length,
      topPrefixes,
    });
  }
  
  return result;
}

/**
 * Get a smart layer name for a specific relative layer.
 * Returns the computed name if available, otherwise falls back to generic.
 */
export function getSmartLayerName(
  relativeLayer: number,
  smartLayerNames: Map<number, SmartLayerName> | null
): string {
  if (smartLayerNames?.has(relativeLayer)) {
    return smartLayerNames.get(relativeLayer)!.name;
  }
  return getRelativeLayerName(relativeLayer);
}

// ============================================================================
// Incremental Layout
// ============================================================================

/**
 * Configuration for incremental layout decisions
 */
const INCREMENTAL_LAYOUT_CONFIG = {
  changeThreshold: 0.2,        // If >20% nodes changed, do full recompute
  collisionPadding: 20,        // Minimum distance between node centers
  maxCollisionIterations: 50,  // Max iterations for collision resolution
  neighborWeight: 0.8,         // Weight for neighbor-based positioning
};

/**
 * Represents the diff between current and previous graph state
 */
export interface LayoutDiff {
  added: LayoutNode[];
  removed: string[];
  unchanged: LayoutNode[];
  changedEdges: boolean;
}

/**
 * Extended position with additional metadata for incremental updates
 */
export interface IncrementalPosition extends NodePosition {
  isNew: boolean;
  wasRepositioned: boolean;
}

/**
 * Result of incremental layout computation
 */
export interface IncrementalLayoutResult extends LayoutResult {
  usedIncremental: boolean;
  addedCount: number;
  removedCount: number;
}

/**
 * Compute the diff between current nodes and previously laid-out nodes.
 */
export function computeLayoutDiff(
  currentNodes: LayoutNode[],
  currentEdges: LayoutEdge[],
  previousPositions: Map<string, NodePosition>,
  previousEdgeCount: number
): LayoutDiff {
  const previousNodeIds = new Set(previousPositions.keys());
  const currentNodeIds = new Set(currentNodes.map(n => n.id));
  
  const added: LayoutNode[] = [];
  const unchanged: LayoutNode[] = [];
  const removed: string[] = [];
  
  // Find added and unchanged nodes
  for (const node of currentNodes) {
    if (previousNodeIds.has(node.id)) {
      unchanged.push(node);
    } else {
      added.push(node);
    }
  }
  
  // Find removed nodes
  for (const nodeId of previousNodeIds) {
    if (!currentNodeIds.has(nodeId)) {
      removed.push(nodeId);
    }
  }
  
  // Check if edge count changed significantly (simple heuristic for edge changes)
  const changedEdges = Math.abs(currentEdges.length - previousEdgeCount) > previousEdgeCount * 0.1;
  
  return { added, removed, unchanged, changedEdges };
}

/**
 * Determine if we should use incremental layout or do a full recompute.
 */
export function shouldUseIncrementalLayout(
  diff: LayoutDiff,
  totalNodes: number
): boolean {
  // Don't use incremental if edges changed significantly
  if (diff.changedEdges) return false;
  
  // Don't use incremental if no previous layout exists
  if (diff.unchanged.length === 0) return false;
  
  // Calculate change ratio
  const changedCount = diff.added.length + diff.removed.length;
  const changeRatio = changedCount / Math.max(totalNodes, 1);
  
  return changeRatio <= INCREMENTAL_LAYOUT_CONFIG.changeThreshold;
}

/**
 * Find connected neighbors of a node from edges.
 */
function findNeighbors(
  nodeId: string,
  edges: LayoutEdge[],
  validNodes: Set<string>
): string[] {
  const neighbors: string[] = [];
  
  for (const edge of edges) {
    if (edge.from === nodeId && validNodes.has(edge.to)) {
      neighbors.push(edge.to);
    }
    if (edge.to === nodeId && validNodes.has(edge.from)) {
      neighbors.push(edge.from);
    }
  }
  
  return neighbors;
}

/**
 * Position a new node based on its neighbors' positions.
 */
function positionNewNode(
  node: LayoutNode,
  edges: LayoutEdge[],
  existingPositions: Map<string, NodePosition>,
  layers: Map<string, number>
): NodePosition {
  const layer = layers.get(node.id) ?? 0;
  const neighbors = findNeighbors(node.id, edges, new Set(existingPositions.keys()));
  
  if (neighbors.length === 0) {
    // No neighbors - position at layer's X with some offset
    const layerX = LAYOUT_CONFIG.marginX + layer * LAYOUT_CONFIG.rankSep;
    const existingYs = [...existingPositions.values()]
      .filter(p => p.layer === layer)
      .map(p => p.y);
    
    const maxY = existingYs.length > 0 ? Math.max(...existingYs) : 0;
    return {
      x: layerX,
      y: maxY + LAYOUT_CONFIG.nodeSep + LAYOUT_CONFIG.nodeHeight,
      layer,
    };
  }
  
  // Calculate weighted average of neighbor positions
  let totalX = 0;
  let totalY = 0;
  let upstreamCount = 0;
  let downstreamCount = 0;
  
  for (const neighborId of neighbors) {
    const pos = existingPositions.get(neighborId)!;
    totalX += pos.x;
    totalY += pos.y;
    
    // Track direction for X offset
    const neighborLayer = pos.layer;
    if (neighborLayer < layer) upstreamCount++;
    else if (neighborLayer > layer) downstreamCount++;
  }
  
  const avgX = totalX / neighbors.length;
  const avgY = totalY / neighbors.length;
  
  // Offset X based on layer position
  const layerX = LAYOUT_CONFIG.marginX + layer * LAYOUT_CONFIG.rankSep;
  const x = layerX * INCREMENTAL_LAYOUT_CONFIG.neighborWeight + 
            avgX * (1 - INCREMENTAL_LAYOUT_CONFIG.neighborWeight);
  
  return { x, y: avgY, layer };
}

/**
 * Resolve collisions between nodes using simple force-directed adjustment.
 */
function resolveCollisions(
  positions: Map<string, NodePosition>
): Map<string, NodePosition> {
  const result = new Map(positions);
  const nodeIds = [...result.keys()];
  
  for (let iteration = 0; iteration < INCREMENTAL_LAYOUT_CONFIG.maxCollisionIterations; iteration++) {
    let hadCollision = false;
    
    for (let i = 0; i < nodeIds.length; i++) {
      for (let j = i + 1; j < nodeIds.length; j++) {
        const posA = result.get(nodeIds[i])!;
        const posB = result.get(nodeIds[j])!;
        
        // Only resolve collisions within same layer
        if (posA.layer !== posB.layer) continue;
        
        const dx = posB.x - posA.x;
        const dy = posB.y - posA.y;
        const distance = Math.sqrt(dx * dx + dy * dy);
        
        const minDistance = LAYOUT_CONFIG.nodeHeight + INCREMENTAL_LAYOUT_CONFIG.collisionPadding;
        
        if (distance < minDistance && distance > 0) {
          hadCollision = true;
          
          // Push nodes apart vertically (within layer)
          const overlap = minDistance - distance;
          const pushY = (overlap / 2) * (dy / distance || 1);
          
          result.set(nodeIds[i], { ...posA, y: posA.y - pushY });
          result.set(nodeIds[j], { ...posB, y: posB.y + pushY });
        }
      }
    }
    
    if (!hadCollision) break;
  }
  
  return result;
}

/**
 * Perform incremental layout update.
 * Only computes positions for new nodes based on their neighbors.
 * Preserves positions of unchanged nodes.
 */
export function incrementalLayout(
  diff: LayoutDiff,
  existingPositions: Map<string, NodePosition>,
  edges: LayoutEdge[]
): IncrementalLayoutResult {
  const allNodes = [...diff.unchanged, ...diff.added];
  
  // Compute new topological layers (needed for new nodes)
  const layers = computeTopologicalLayers(allNodes, edges);
  
  // Start with existing positions for unchanged nodes
  const positions = new Map<string, NodePosition>();
  
  for (const node of diff.unchanged) {
    const existing = existingPositions.get(node.id);
    if (existing) {
      // Update layer if it changed
      const newLayer = layers.get(node.id) ?? existing.layer;
      positions.set(node.id, { ...existing, layer: newLayer });
    }
  }
  
  // Position new nodes based on neighbors
  for (const node of diff.added) {
    const pos = positionNewNode(node, edges, positions, layers);
    positions.set(node.id, pos);
  }
  
  // Resolve any collisions
  const resolvedPositions = resolveCollisions(positions);
  
  // Compute bounds
  let minX = Infinity, maxX = -Infinity;
  let minY = Infinity, maxY = -Infinity;
  
  for (const pos of resolvedPositions.values()) {
    minX = Math.min(minX, pos.x);
    maxX = Math.max(maxX, pos.x);
    minY = Math.min(minY, pos.y);
    maxY = Math.max(maxY, pos.y);
  }
  
  return {
    positions: resolvedPositions,
    layers,
    bounds: {
      minX: minX === Infinity ? 0 : minX,
      maxX: maxX === -Infinity ? 0 : maxX,
      minY: minY === Infinity ? 0 : minY,
      maxY: maxY === -Infinity ? 0 : maxY,
    },
    usedIncremental: true,
    addedCount: diff.added.length,
    removedCount: diff.removed.length,
  };
}

/**
 * Smart layout function that decides between full and incremental layout.
 * Use this as the main entry point for layout computation.
 */
export function computeLayoutWithDiff(
  nodes: LayoutNode[],
  edges: LayoutEdge[],
  previousPositions: Map<string, NodePosition> | null,
  previousEdgeCount: number
): IncrementalLayoutResult {
  // If no previous layout, do full compute
  if (!previousPositions || previousPositions.size === 0) {
    const result = precomputeLayout(nodes, edges);
    return {
      ...result,
      usedIncremental: false,
      addedCount: nodes.length,
      removedCount: 0,
    };
  }
  
  // Compute diff
  const diff = computeLayoutDiff(nodes, edges, previousPositions, previousEdgeCount);
  
  // Decide: incremental or full?
  if (shouldUseIncrementalLayout(diff, nodes.length)) {
    return incrementalLayout(diff, previousPositions, edges);
  }
  
  // Full recompute
  const result = precomputeLayout(nodes, edges);
  return {
    ...result,
    usedIncremental: false,
    addedCount: diff.added.length,
    removedCount: diff.removed.length,
  };
}


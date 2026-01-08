/**
 * Visibility Logic Layer
 * 
 * Determines which nodes and edges are visible based on:
 * - Anchor node selection (lineage-based filtering)
 * - Flow lens (restricts to flow members)
 * - Depth controls (upstream/downstream hop limits)
 * 
 * This layer is strictly separate from rendering - it only computes
 * WHAT should be shown, not HOW it should be drawn.
 */

import type { GraphNode, GraphEdge, GraphFlow } from "../types";

// ============================================================================
// Types
// ============================================================================

export interface VisibilityState {
  anchor: string | null;           // Origin node ID - always visible
  focus: string | null;            // Current exploration focus (stretches view in that direction)
  flow: string | null;             // Flow ID (optional lens)
  upstreamDepth: number;           // Hops upstream from focus/anchor (default: 3)
  downstreamDepth: number;         // Hops downstream from focus/anchor (default: 2)
  showOrphans: boolean;            // Nodes with no connections (default: false)
}

export type VisibilityReason =
  | { type: "anchor" }
  | { type: "upstream"; hops: number; path: string[] }
  | { type: "downstream"; hops: number; path: string[] }
  | { type: "flow_member"; flowName: string };

export interface VisibleNode extends GraphNode {
  visibilityReason: VisibilityReason;
  relativeLayer: number; // Distance from anchor (negative = upstream, positive = downstream)
}

export interface VisibilityResult {
  visibleNodes: VisibleNode[];
  visibleEdges: GraphEdge[];
  anchorNode: GraphNode | null;
  layerRange: { min: number; max: number };
}

export const DEFAULT_VISIBILITY_STATE: VisibilityState = {
  anchor: null,
  focus: null,
  flow: null,
  upstreamDepth: 3,
  downstreamDepth: 2,
  showOrphans: false,
};

// Performance limits
export const PERFORMANCE_LIMITS = {
  MAX_VISIBLE_NODES: 300,
  MAX_EDGES_PER_NODE: 50,
};

// ============================================================================
// Default Node Selection
// ============================================================================

/**
 * Find the best default node for a flow, biasing towards population tables.
 * Population tables represent the entry point/starting population of a pipeline.
 * 
 * Priority order:
 * 1. Population table matching flow name keywords
 * 2. Any population table in the flow
 * 3. Staging table matching flow name keywords
 * 4. Fallback to first anchor node
 */
export function findDefaultNodeForFlow(
  flow: GraphFlow,
  allNodes: GraphNode[]
): string | null {
  const memberNodes = allNodes.filter(n => flow.memberNodes.includes(n.id));
  const flowKeywords = flow.name.toLowerCase().split(/[\s_-]+/);
  
  // Priority 1: Population tables matching flow name
  const populationMatch = memberNodes.find(n => 
    n.name.toLowerCase().includes('_population') &&
    flowKeywords.some(kw => kw.length > 2 && n.name.toLowerCase().includes(kw))
  );
  if (populationMatch) return populationMatch.id;
  
  // Priority 2: Any population table in the flow
  const anyPopulation = memberNodes.find(n => 
    n.name.toLowerCase().includes('_population')
  );
  if (anyPopulation) return anyPopulation.id;
  
  // Priority 3: Staging table matching flow name
  const stagingMatch = memberNodes.find(n =>
    n.name.toLowerCase().startsWith('stg_') &&
    flowKeywords.some(kw => kw.length > 2 && n.name.toLowerCase().includes(kw))
  );
  if (stagingMatch) return stagingMatch.id;
  
  // Fallback: First anchor node
  return flow.anchorNodes[0] ?? null;
}

// ============================================================================
// Lineage Computation (BFS)
// ============================================================================

interface LineageNode {
  id: string;
  hops: number;
  path: string[];
  direction: "upstream" | "downstream";
}

/**
 * Compute lineage from an anchor node using BFS.
 * Returns nodes reachable within the specified depth limits.
 */
export function computeLineage(
  anchorId: string,
  nodes: GraphNode[],
  edges: GraphEdge[],
  upstreamDepth: number,
  downstreamDepth: number,
  candidateNodeIds?: Set<string>
): Map<string, LineageNode> {
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));
  const lineage = new Map<string, LineageNode>();

  // Build adjacency maps
  const upstreamEdges = new Map<string, GraphEdge[]>(); // to -> from[]
  const downstreamEdges = new Map<string, GraphEdge[]>(); // from -> to[]

  for (const edge of edges) {
    // Skip edges to nodes outside candidate set
    if (candidateNodeIds) {
      if (!candidateNodeIds.has(edge.from) || !candidateNodeIds.has(edge.to)) {
        continue;
      }
    }

    if (!upstreamEdges.has(edge.to)) upstreamEdges.set(edge.to, []);
    if (!downstreamEdges.has(edge.from)) downstreamEdges.set(edge.from, []);
    upstreamEdges.get(edge.to)!.push(edge);
    downstreamEdges.get(edge.from)!.push(edge);
  }

  // BFS for upstream nodes
  if (upstreamDepth > 0) {
    const queue: string[] = [anchorId];
    const visited = new Set<string>([anchorId]);

    while (queue.length > 0) {
      const currentId = queue.shift()!;
      // Look up CURRENT hops from lineage (may have been updated since queuing)
      const currentHops = lineage.get(currentId)?.hops ?? 0;

      const currentPath = lineage.get(currentId)?.path ?? [anchorId];
      const edges = upstreamEdges.get(currentId) || [];
      for (const edge of edges) {
        const existingNode = lineage.get(edge.from);
        const newHops = currentHops + 1;

        // Update hops if this path is longer (even for existing nodes)
        // This ensures nodes with multiple children are placed at max(child layers) + 1
        if (existingNode && newHops > existingNode.hops) {
          // Update existing node's hops to the longer path
          lineage.set(edge.from, {
            ...existingNode,
            hops: newHops,
            path: [...currentPath, edge.from],
          });
        } else if (!existingNode && newHops <= upstreamDepth) {
          // Only ADD new nodes if within depth limit
          const node: LineageNode = {
            id: edge.from,
            hops: newHops,
            path: [...currentPath, edge.from],
            direction: "upstream",
          };
          lineage.set(edge.from, node);
        }

        // Add to queue if not yet queued (to process its edges for hop updates)
        if (!visited.has(edge.from) && lineage.has(edge.from)) {
          visited.add(edge.from);
          queue.push(edge.from);
        }
      }
    }
  }

  // BFS for downstream nodes
  if (downstreamDepth > 0) {
    const queue: string[] = [anchorId];
    const visited = new Set<string>([anchorId]);

    while (queue.length > 0) {
      const currentId = queue.shift()!;
      // Look up CURRENT hops from lineage (may have been updated since queuing)
      const currentHops = lineage.get(currentId)?.hops ?? 0;

      const currentPath = lineage.get(currentId)?.path ?? [anchorId];
      const edges = downstreamEdges.get(currentId) || [];
      for (const edge of edges) {
        const existingNode = lineage.get(edge.to);
        const newHops = currentHops + 1;

        // Update hops if this path is longer (even for existing nodes)
        // This ensures nodes with multiple parents are placed at max(parent layers) + 1
        if (existingNode && newHops > existingNode.hops) {
          // Update existing node's hops to the longer path
          lineage.set(edge.to, {
            ...existingNode,
            hops: newHops,
            path: [...currentPath, edge.to],
          });
        } else if (!existingNode && newHops <= downstreamDepth) {
          // Only ADD new nodes if within depth limit
          const node: LineageNode = {
            id: edge.to,
            hops: newHops,
            path: [...currentPath, edge.to],
            direction: "downstream",
          };
          lineage.set(edge.to, node);
        }

        // Add to queue if not yet queued (to process its edges for hop updates)
        if (!visited.has(edge.to) && lineage.has(edge.to)) {
          visited.add(edge.to);
          queue.push(edge.to);
        }
      }
    }
  }

  return lineage;
}

// ============================================================================
// Visibility Computation
// ============================================================================

/**
 * Compute which nodes and edges should be visible based on the current state.
 * This is the main entry point for visibility logic.
 */
export function computeVisibility(
  state: VisibilityState,
  allNodes: GraphNode[],
  allEdges: GraphEdge[],
  flows: GraphFlow[]
): VisibilityResult {
  const nodeMap = new Map(allNodes.map((n) => [n.id, n]));

  // Empty state - no anchor and no flow
  if (!state.anchor && !state.flow) {
    return {
      visibleNodes: [],
      visibleEdges: [],
      anchorNode: null,
      layerRange: { min: 0, max: 0 },
    };
  }

  // Get candidate nodes (flow members or all nodes)
  let candidateNodeIds: Set<string>;
  let flowName: string | undefined;

  if (state.flow) {
    const flow = flows.find((f) => f.id === state.flow);
    if (flow) {
      candidateNodeIds = new Set(flow.memberNodes);
      flowName = flow.name;
    } else {
      candidateNodeIds = new Set(allNodes.map((n) => n.id));
    }
  } else {
    candidateNodeIds = new Set(allNodes.map((n) => n.id));
  }

  // If only flow (no anchor), return all flow members
  if (!state.anchor && state.flow) {
    const visibleNodes: VisibleNode[] = [];
    for (const nodeId of candidateNodeIds) {
      const node = nodeMap.get(nodeId);
      if (node) {
        visibleNodes.push({
          ...node,
          visibilityReason: { type: "flow_member", flowName: flowName! },
          relativeLayer: node.layoutLayer ?? 0,
        });
      }
    }

    const visibleNodeIds = new Set(visibleNodes.map((n) => n.id));
    const visibleEdges = allEdges.filter(
      (e) => visibleNodeIds.has(e.from) && visibleNodeIds.has(e.to)
    );

    const layers = visibleNodes.map((n) => n.relativeLayer);
    return {
      visibleNodes,
      visibleEdges,
      anchorNode: null,
      layerRange: {
        min: Math.min(...layers, 0),
        max: Math.max(...layers, 0),
      },
    };
  }

  // Anchor is specified - compute lineage
  const anchorNode = nodeMap.get(state.anchor!);
  if (!anchorNode) {
    return {
      visibleNodes: [],
      visibleEdges: [],
      anchorNode: null,
      layerRange: { min: 0, max: 0 },
    };
  }

  // Determine focus node (for stretching exploration)
  const focusNode = state.focus ? nodeMap.get(state.focus) : null;
  
  // Build edge maps for path finding
  const upstreamEdges = new Map<string, Set<string>>();  // to -> from[]
  const downstreamEdges = new Map<string, Set<string>>(); // from -> to[]
  for (const edge of allEdges) {
    if (!upstreamEdges.has(edge.to)) upstreamEdges.set(edge.to, new Set());
    if (!downstreamEdges.has(edge.from)) downstreamEdges.set(edge.from, new Set());
    upstreamEdges.get(edge.to)!.add(edge.from);
    downstreamEdges.get(edge.from)!.add(edge.to);
  }

  // Find path from anchor to focus (BFS)
  function findPath(fromId: string, toId: string, direction: "upstream" | "downstream"): string[] | null {
    const edgeMap = direction === "upstream" ? upstreamEdges : downstreamEdges;
    const queue: { id: string; path: string[] }[] = [{ id: fromId, path: [fromId] }];
    const visited = new Set<string>([fromId]);

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (current.id === toId) return current.path;

      const neighbors = edgeMap.get(current.id) || new Set();
      for (const neighbor of neighbors) {
        if (visited.has(neighbor)) continue;
        visited.add(neighbor);
        queue.push({ id: neighbor, path: [...current.path, neighbor] });
      }
    }
    return null;
  }

  // Compute base lineage from anchor (or from focus with path to anchor)
  let lineage: Map<string, { id: string; hops: number; path: string[]; direction: "upstream" | "downstream" }>;
  let anchorToFocusPath: string[] = [];
  let focusDirection: "upstream" | "downstream" | null = null;

  if (focusNode && state.focus !== state.anchor) {
    // Stretching exploration: find path from anchor to focus
    anchorToFocusPath = findPath(state.anchor!, state.focus!, "upstream") || [];
    if (anchorToFocusPath.length > 0) {
      focusDirection = "upstream";
    } else {
      anchorToFocusPath = findPath(state.anchor!, state.focus!, "downstream") || [];
      if (anchorToFocusPath.length > 0) {
        focusDirection = "downstream";
      }
    }

    if (focusDirection) {
      // STRETCHING: Compute lineage from BOTH anchor AND focus, then merge
      // This keeps the anchor's other connections visible while extending in the focus direction
      
      // 1. Compute anchor's lineage (keeps other connections visible)
      const anchorLineage = computeLineage(
        state.anchor!,
        allNodes,
        allEdges,
        state.upstreamDepth,
        state.downstreamDepth,
        undefined
      );
      
      // 2. Compute focus's extended lineage in the stretch direction
      // The focus is N hops from anchor, so we extend N + upstreamDepth further
      const focusHops = anchorToFocusPath.length - 1;
      const extendedUpstreamDepth = focusDirection === "upstream" ? state.upstreamDepth : 0;
      const extendedDownstreamDepth = focusDirection === "downstream" ? state.downstreamDepth : 0;
      
      const focusLineage = computeLineage(
        state.focus!,
        allNodes,
        allEdges,
        extendedUpstreamDepth,
        extendedDownstreamDepth,
        undefined
      );
      
      // 3. Merge lineages - anchor lineage + focus's extended lineage
      lineage = new Map(anchorLineage);
      for (const [nodeId, info] of focusLineage) {
        if (!lineage.has(nodeId)) {
          // Adjust hops to be relative to anchor, not focus
          const adjustedHops = focusHops + info.hops;
          lineage.set(nodeId, {
            ...info,
            hops: adjustedHops,
          });
        }
      }
    } else {
      // Focus not reachable from anchor, fall back to anchor-only exploration
      lineage = computeLineage(
        state.anchor!,
        allNodes,
        allEdges,
        state.upstreamDepth,
        state.downstreamDepth,
        undefined // Don't restrict - filter by flow after
      );
    }
  } else {
    // No focus or same as anchor - standard exploration from anchor
    // DON'T restrict traversal - we'll filter by flow membership when building visible nodes
    lineage = computeLineage(
      state.anchor!,
      allNodes,
      allEdges,
      state.upstreamDepth,
      state.downstreamDepth,
      undefined // Don't restrict - filter by flow after
    );
  }

  // Build visible nodes
  const visibleNodes: VisibleNode[] = [];
  const lineageNodeIds = new Set(lineage.keys());
  const visibleNodeIds = new Set<string>();

  // Add anchor node at layer 0
  visibleNodes.push({
    ...anchorNode,
    visibilityReason: { type: "anchor" },
    relativeLayer: 0,
  });
  visibleNodeIds.add(state.anchor!);

  // If stretching, add path from anchor to focus first
  // All nodes in the lineage path are visible - they are "in the flow" by definition
  if (anchorToFocusPath.length > 1 && focusDirection) {
    for (let i = 1; i < anchorToFocusPath.length; i++) {
      const pathNodeId = anchorToFocusPath[i];
      if (visibleNodeIds.has(pathNodeId)) continue;
      
      const pathNode = nodeMap.get(pathNodeId);
      if (!pathNode) continue;

      const relativeLayer = focusDirection === "upstream" ? -i : i;
      visibleNodes.push({
        ...pathNode,
        visibilityReason: {
          type: focusDirection === "upstream" ? "upstream" : "downstream",
          hops: i,
          path: anchorToFocusPath.slice(0, i + 1),
        },
        relativeLayer,
      });
      visibleNodeIds.add(pathNodeId);
    }
  }

  // Calculate the base layer offset for focus-based lineage
  // If focus is N hops from anchor, its lineage nodes are at N + their hops
  const focusHopsFromAnchor = anchorToFocusPath.length > 0 ? anchorToFocusPath.length - 1 : 0;

  // Add lineage nodes from focus/anchor exploration
  // All nodes in the lineage are visible - they are "in the flow" by virtue of being connected to the anchor
  for (const [nodeId, lineageInfo] of lineage) {
    if (visibleNodeIds.has(nodeId)) continue;
    
    const node = nodeMap.get(nodeId);
    if (!node) continue;

    // Calculate relative layer based on whether we're stretching
    let relativeLayer: number;
    if (focusDirection) {
      // Stretching: layers are relative to anchor, not focus
      if (focusDirection === "upstream") {
        // Focus is upstream of anchor, so focus's upstream is further negative
        relativeLayer = -(focusHopsFromAnchor + lineageInfo.hops);
      } else {
        // Focus is downstream of anchor
        relativeLayer = focusHopsFromAnchor + lineageInfo.hops;
      }
    } else {
      // Normal exploration from anchor
      relativeLayer = lineageInfo.direction === "upstream" 
        ? -lineageInfo.hops 
        : lineageInfo.hops;
    }

    const visibleNode: VisibleNode = {
      ...node,
      visibilityReason:
        lineageInfo.direction === "upstream"
          ? { type: "upstream", hops: Math.abs(relativeLayer), path: lineageInfo.path }
          : { type: "downstream", hops: Math.abs(relativeLayer), path: lineageInfo.path },
      relativeLayer,
    };

    visibleNodes.push(visibleNode);
    visibleNodeIds.add(nodeId);
  }

  // Apply performance limits
  let finalNodes = visibleNodes;
  if (visibleNodes.length > PERFORMANCE_LIMITS.MAX_VISIBLE_NODES) {
    // Prioritize nodes closer to anchor
    finalNodes = visibleNodes
      .sort((a, b) => Math.abs(a.relativeLayer) - Math.abs(b.relativeLayer))
      .slice(0, PERFORMANCE_LIMITS.MAX_VISIBLE_NODES);
  }

  // Compute visible edges (between visible nodes only)
  const finalNodeIds = new Set(finalNodes.map((n) => n.id));
  const visibleEdges = allEdges.filter(
    (e) => finalNodeIds.has(e.from) && finalNodeIds.has(e.to)
  );

  // Compute layer range
  const layers = finalNodes.map((n) => n.relativeLayer);
  const layerRange = {
    min: Math.min(...layers, 0),
    max: Math.max(...layers, 0),
  };

  return {
    visibleNodes: finalNodes,
    visibleEdges,
    anchorNode,
    layerRange,
  };
}

/**
 * Get a human-readable description of why a node is visible.
 */
export function getVisibilityDescription(reason: VisibilityReason): string {
  switch (reason.type) {
    case "anchor":
      return "Selected table";
    case "upstream":
      return `${reason.hops} hop${reason.hops > 1 ? "s" : ""} upstream`;
    case "downstream":
      return `${reason.hops} hop${reason.hops > 1 ? "s" : ""} downstream`;
    case "flow_member":
      return `In ${reason.flowName} flow`;
  }
}

/**
 * Get edges that connect to/from a specific node.
 */
export function getNodeEdges(
  nodeId: string,
  edges: GraphEdge[]
): { incoming: GraphEdge[]; outgoing: GraphEdge[] } {
  return {
    incoming: edges.filter((e) => e.to === nodeId),
    outgoing: edges.filter((e) => e.from === nodeId),
  };
}


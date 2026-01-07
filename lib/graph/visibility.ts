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
  anchor: string | null;           // Selected node ID
  flow: string | null;             // Flow ID (optional lens)
  upstreamDepth: number;           // Hops upstream from anchor (default: 3)
  downstreamDepth: number;         // Hops downstream from anchor (default: 2)
  showOrphans: boolean;            // Nodes with no connections (default: false)
}

export type VisibilityReason =
  | { type: "anchor" }
  | { type: "upstream"; hops: number; path: string[] }
  | { type: "downstream"; hops: number; path: string[] }
  | { type: "flow_member"; flowName: string }
  | { type: "ghost"; reason: string };

export interface VisibleNode extends GraphNode {
  visibilityReason: VisibilityReason;
  relativeLayer: number; // Distance from anchor (negative = upstream, positive = downstream)
}

export interface VisibilityResult {
  visibleNodes: VisibleNode[];
  visibleEdges: GraphEdge[];
  ghostNodes: VisibleNode[];  // Nodes in lineage but outside flow
  anchorNode: GraphNode | null;
  layerRange: { min: number; max: number };
}

export const DEFAULT_VISIBILITY_STATE: VisibilityState = {
  anchor: null,
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
    const queue: LineageNode[] = [
      { id: anchorId, hops: 0, path: [anchorId], direction: "upstream" },
    ];
    const visited = new Set<string>([anchorId]);

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (current.hops >= upstreamDepth) continue;

      const edges = upstreamEdges.get(current.id) || [];
      for (const edge of edges) {
        if (visited.has(edge.from)) continue;
        visited.add(edge.from);

        const node: LineageNode = {
          id: edge.from,
          hops: current.hops + 1,
          path: [...current.path, edge.from],
          direction: "upstream",
        };
        lineage.set(edge.from, node);
        queue.push(node);
      }
    }
  }

  // BFS for downstream nodes
  if (downstreamDepth > 0) {
    const queue: LineageNode[] = [
      { id: anchorId, hops: 0, path: [anchorId], direction: "downstream" },
    ];
    const visited = new Set<string>([anchorId]);

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (current.hops >= downstreamDepth) continue;

      const edges = downstreamEdges.get(current.id) || [];
      for (const edge of edges) {
        if (visited.has(edge.to)) continue;
        visited.add(edge.to);

        const node: LineageNode = {
          id: edge.to,
          hops: current.hops + 1,
          path: [...current.path, edge.to],
          direction: "downstream",
        };
        lineage.set(edge.to, node);
        queue.push(node);
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
      ghostNodes: [],
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
      ghostNodes: [],
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
      ghostNodes: [],
      anchorNode: null,
      layerRange: { min: 0, max: 0 },
    };
  }

  // Compute lineage from anchor
  const lineage = computeLineage(
    state.anchor!,
    allNodes,
    allEdges,
    state.upstreamDepth,
    state.downstreamDepth,
    state.flow ? undefined : undefined // Don't restrict lineage traversal by flow
  );

  // Build visible nodes and ghost nodes
  const visibleNodes: VisibleNode[] = [];
  const ghostNodes: VisibleNode[] = [];
  const anchorLayer = anchorNode.layoutLayer ?? 0;

  // Add anchor node
  visibleNodes.push({
    ...anchorNode,
    visibilityReason: { type: "anchor" },
    relativeLayer: 0,
  });

  // Add lineage nodes
  for (const [nodeId, lineageInfo] of lineage) {
    const node = nodeMap.get(nodeId);
    if (!node) continue;

    const isInFlow = !state.flow || candidateNodeIds.has(nodeId);
    const nodeLayer = node.layoutLayer ?? 0;
    const relativeLayer =
      lineageInfo.direction === "upstream"
        ? -lineageInfo.hops
        : lineageInfo.hops;

    const visibleNode: VisibleNode = {
      ...node,
      visibilityReason:
        lineageInfo.direction === "upstream"
          ? { type: "upstream", hops: lineageInfo.hops, path: lineageInfo.path }
          : { type: "downstream", hops: lineageInfo.hops, path: lineageInfo.path },
      relativeLayer,
    };

    if (isInFlow) {
      visibleNodes.push(visibleNode);
    } else {
      // Ghost node - in lineage but outside flow
      ghostNodes.push({
        ...visibleNode,
        visibilityReason: {
          type: "ghost",
          reason: `In lineage but outside ${flowName} flow`,
        },
      });
    }
  }

  // Apply performance limits
  let finalNodes = visibleNodes;
  if (visibleNodes.length > PERFORMANCE_LIMITS.MAX_VISIBLE_NODES) {
    // Prioritize nodes closer to anchor
    finalNodes = visibleNodes
      .sort((a, b) => Math.abs(a.relativeLayer) - Math.abs(b.relativeLayer))
      .slice(0, PERFORMANCE_LIMITS.MAX_VISIBLE_NODES);
  }

  // Compute visible edges (only between visible nodes)
  const visibleNodeIds = new Set(finalNodes.map((n) => n.id));
  const visibleEdges = allEdges.filter(
    (e) => visibleNodeIds.has(e.from) && visibleNodeIds.has(e.to)
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
    ghostNodes: ghostNodes.slice(0, 50), // Limit ghost nodes too
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
    case "ghost":
      return reason.reason;
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


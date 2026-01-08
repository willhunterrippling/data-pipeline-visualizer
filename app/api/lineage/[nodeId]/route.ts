import { NextRequest, NextResponse } from "next/server";
import { 
  getNodes, 
  getEdges, 
  getFlows, 
  getNodeById,
  generateLineageCacheKey,
  getLineageCache,
  setLineageCache,
} from "@/lib/db";
import {
  computeVisibility,
  getVisibilityDescription,
  type VisibilityState,
  type VisibilityReason,
} from "@/lib/graph/visibility";
import { getRelativeLayerName, computeSmartLayerNames, type SmartLayerName } from "@/lib/graph/layout";
import type { GraphNode, GraphEdge, GraphFlow, NodeMetadata, EdgeMetadata } from "@/lib/types";
import type { VisibleNode } from "@/lib/graph/visibility";

// Layout constants for local lineage layout (matches layout.ts)
const LOCAL_LAYOUT_CONFIG = {
  nodeSep: 60,      // Vertical spacing between nodes in same layer
  rankSep: 200,     // Horizontal spacing between layers
  marginX: 50,
  marginY: 50,
  nodeHeight: 50,
};

/**
 * Compute local layout positions for lineage subset based on relativeLayer.
 * This creates a compact layout centered around the anchor (layer 0).
 */
function computeLocalLayout<T extends VisibleNode>(
  nodes: T[],
  layerRange: { min: number; max: number }
): T[] {
  if (nodes.length === 0) return nodes;

  // Group nodes by relativeLayer
  const nodesByLayer = new Map<number, T[]>();
  for (const node of nodes) {
    const layer = node.relativeLayer;
    if (!nodesByLayer.has(layer)) {
      nodesByLayer.set(layer, []);
    }
    nodesByLayer.get(layer)!.push(node);
  }

  // Sort nodes within each layer by name for consistent ordering
  for (const layerNodes of nodesByLayer.values()) {
    layerNodes.sort((a, b) => a.name.localeCompare(b.name));
  }

  // Compute offset to shift layers so min layer starts at margin
  // e.g., if layerRange.min = -3, we add 3 to get layer 0 at position 3
  const layerOffset = -layerRange.min;

  // Compute positions for each node
  const positionedNodes: T[] = [];

  for (const node of nodes) {
    const layer = node.relativeLayer;
    const layerNodes = nodesByLayer.get(layer)!;
    const indexInLayer = layerNodes.indexOf(node);

    // X position based on layer (left-to-right flow)
    const layoutX = LOCAL_LAYOUT_CONFIG.marginX + 
                    (layer + layerOffset) * LOCAL_LAYOUT_CONFIG.rankSep;

    // Y position based on index within layer
    const layoutY = LOCAL_LAYOUT_CONFIG.marginY + 
                    indexInLayer * (LOCAL_LAYOUT_CONFIG.nodeHeight + LOCAL_LAYOUT_CONFIG.nodeSep);

    positionedNodes.push({
      ...node,
      layoutX,
      layoutY,
    });
  }

  return positionedNodes;
}

export interface LineageResponse {
  anchor: GraphNode;
  nodes: Array<GraphNode & { visibilityReason: VisibilityReason; relativeLayer: number }>;
  edges: GraphEdge[];
  layers: Record<string, { layer: number; name: string }>;
  smartLayerNames: Record<number, SmartLayerName>;  // Smart names keyed by relative layer
  visibilityReasons: Record<string, { reason: VisibilityReason; description: string }>;
  stats: {
    totalNodes: number;
    visibleNodes: number;
    layerRange: { min: number; max: number };
  };
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ nodeId: string }> }
) {
  const { nodeId } = await params;
  const decodedId = decodeURIComponent(nodeId);
  const searchParams = request.nextUrl.searchParams;

  // Parse query parameters
  const upstreamDepth = parseInt(searchParams.get("upstreamDepth") || "3", 10);
  const downstreamDepth = parseInt(searchParams.get("downstreamDepth") || "2", 10);
  const flowId = searchParams.get("flowId") || null;
  const focusId = searchParams.get("focusId") || null; // For stretching exploration

  try {
    // Verify anchor node exists
    const anchorDb = getNodeById(decodedId);
    if (!anchorDb) {
      return NextResponse.json({ error: "Anchor node not found" }, { status: 404 });
    }

    // Include focusId in cache key
    const cacheKey = generateLineageCacheKey(decodedId, upstreamDepth, downstreamDepth, flowId) + 
      (focusId ? `:focus:${focusId}` : "");
    const cached = getLineageCache(cacheKey);
    
    if (cached) {
      // Return cached response with cache hit header
      const response = NextResponse.json(JSON.parse(cached.result));
      response.headers.set("X-Cache", "HIT");
      response.headers.set("X-Cache-Age", cached.created_at);
      return response;
    }

    // Cache miss - compute lineage
    // Load all data
    const dbNodes = getNodes();
    const dbEdges = getEdges();
    const dbFlows = getFlows();

    // Convert to API types
    const allNodes: GraphNode[] = dbNodes.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      subtype: n.subtype as GraphNode["subtype"],
      groupId: n.group_id || undefined,
      repo: n.repo || undefined,
      metadata: n.metadata ? (JSON.parse(n.metadata) as NodeMetadata) : undefined,
      layoutX: n.layout_x ?? undefined,
      layoutY: n.layout_y ?? undefined,
      layoutLayer: n.layout_layer ?? undefined,
    }));

    const allEdges: GraphEdge[] = dbEdges.map((e) => ({
      id: e.id,
      from: e.from_node,
      to: e.to_node,
      type: e.type as GraphEdge["type"],
      metadata: e.metadata ? (JSON.parse(e.metadata) as EdgeMetadata) : undefined,
    }));

    const flows: GraphFlow[] = dbFlows.map((f) => ({
      id: f.id,
      name: f.name,
      description: f.description || undefined,
      anchorNodes: f.anchor_nodes ? JSON.parse(f.anchor_nodes) : [],
      memberNodes: f.member_nodes ? JSON.parse(f.member_nodes) : [],
      userDefined: f.user_defined === 1,
      inferenceReason: f.inference_reason || undefined,
    }));

    // Build visibility state
    const visibilityState: VisibilityState = {
      anchor: decodedId,
      focus: focusId,  // For stretching exploration
      flow: flowId,
      upstreamDepth,
      downstreamDepth,
      showOrphans: false,
    };

    // Compute visibility
    const result = computeVisibility(visibilityState, allNodes, allEdges, flows);

    // Compute local layout positions for the lineage subset
    // This creates compact positions based on relativeLayer instead of global positions
    const localVisibleNodes = computeLocalLayout(result.visibleNodes, result.layerRange);

    // Compute smart layer names based on node prefixes
    const smartLayerNamesMap = computeSmartLayerNames(
      localVisibleNodes.map(n => ({ id: n.id, name: n.name, relativeLayer: n.relativeLayer })),
      Math.max(Math.abs(result.layerRange.min), Math.abs(result.layerRange.max))
    );

    // Build response
    const layers: Record<string, { layer: number; name: string }> = {};
    const visibilityReasons: Record<string, { reason: VisibilityReason; description: string }> = {};

    // Process visible nodes - use smart layer names
    for (const node of localVisibleNodes) {
      const smartName = smartLayerNamesMap.get(node.relativeLayer);
      layers[node.id] = {
        layer: node.relativeLayer,
        name: smartName?.name || getRelativeLayerName(node.relativeLayer),
      };
      visibilityReasons[node.id] = {
        reason: node.visibilityReason,
        description: getVisibilityDescription(node.visibilityReason),
      };
    }

    // Convert Map to Record for JSON serialization
    const smartLayerNames: Record<number, SmartLayerName> = {};
    for (const [layer, info] of smartLayerNamesMap) {
      smartLayerNames[layer] = info;
    }

    const response: LineageResponse = {
      anchor: result.anchorNode!,
      nodes: localVisibleNodes,
      edges: result.visibleEdges,
      layers,
      smartLayerNames,
      visibilityReasons,
      stats: {
        totalNodes: allNodes.length,
        visibleNodes: result.visibleNodes.length,
        layerRange: result.layerRange,
      },
    };

    // Store in cache for future requests
    setLineageCache(
      cacheKey,
      decodedId,
      upstreamDepth,
      downstreamDepth,
      flowId,
      JSON.stringify(response)
    );

    const jsonResponse = NextResponse.json(response);
    jsonResponse.headers.set("X-Cache", "MISS");
    return jsonResponse;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


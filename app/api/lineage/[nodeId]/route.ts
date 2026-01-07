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

export interface LineageResponse {
  anchor: GraphNode;
  nodes: Array<GraphNode & { visibilityReason: VisibilityReason; relativeLayer: number }>;
  edges: GraphEdge[];
  ghostNodes: Array<GraphNode & { visibilityReason: VisibilityReason; relativeLayer: number }>;
  layers: Record<string, { layer: number; name: string }>;
  smartLayerNames: Record<number, SmartLayerName>;  // Smart names keyed by relative layer
  visibilityReasons: Record<string, { reason: VisibilityReason; description: string }>;
  stats: {
    totalNodes: number;
    visibleNodes: number;
    ghostNodes: number;
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

  try {
    // Verify anchor node exists
    const anchorDb = getNodeById(decodedId);
    if (!anchorDb) {
      return NextResponse.json({ error: "Anchor node not found" }, { status: 404 });
    }

    // Check cache first
    const cacheKey = generateLineageCacheKey(decodedId, upstreamDepth, downstreamDepth, flowId);
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
      flow: flowId,
      upstreamDepth,
      downstreamDepth,
      showOrphans: false,
    };

    // Compute visibility
    const result = computeVisibility(visibilityState, allNodes, allEdges, flows);

    // Compute smart layer names based on node prefixes
    const allVisibleNodes = [...result.visibleNodes, ...result.ghostNodes];
    const smartLayerNamesMap = computeSmartLayerNames(
      allVisibleNodes.map(n => ({ id: n.id, name: n.name, relativeLayer: n.relativeLayer })),
      Math.max(Math.abs(result.layerRange.min), Math.abs(result.layerRange.max))
    );

    // Build response
    const layers: Record<string, { layer: number; name: string }> = {};
    const visibilityReasons: Record<string, { reason: VisibilityReason; description: string }> = {};

    // Process visible nodes - use smart layer names
    for (const node of result.visibleNodes) {
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

    // Process ghost nodes - use smart layer names
    for (const node of result.ghostNodes) {
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
      nodes: result.visibleNodes,
      edges: result.visibleEdges,
      ghostNodes: result.ghostNodes,
      layers,
      smartLayerNames,
      visibilityReasons,
      stats: {
        totalNodes: allNodes.length,
        visibleNodes: result.visibleNodes.length,
        ghostNodes: result.ghostNodes.length,
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


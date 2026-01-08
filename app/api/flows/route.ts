import { NextRequest, NextResponse } from "next/server";
import { v4 as uuid } from "uuid";
import { getFlows, insertFlow, getNodes, getDb, getNodeById } from "@/lib/db";
import type { GraphFlow, GraphNode, NodeMetadata } from "@/lib/types";

// Helper to build member nodes by traversing upstream from anchor
export function buildFlowMembers(anchorNodeId: string, depth: number = 6): string[] {
  const db = getDb();
  const edges = db.prepare("SELECT from_node, to_node FROM edges").all() as Array<{
    from_node: string;
    to_node: string;
  }>;

  // Build upstream map
  const upstreamMap = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (!upstreamMap.has(edge.to_node)) {
      upstreamMap.set(edge.to_node, new Set());
    }
    upstreamMap.get(edge.to_node)!.add(edge.from_node);
  }

  // Traverse upstream from anchor
  const memberSet = new Set<string>();
  
  function getUpstream(nodeId: string, remainingDepth: number, visited: Set<string>): void {
    if (remainingDepth === 0 || visited.has(nodeId)) return;
    visited.add(nodeId);
    memberSet.add(nodeId);

    const upstream = upstreamMap.get(nodeId) || new Set();
    for (const upId of upstream) {
      getUpstream(upId, remainingDepth - 1, visited);
    }
  }

  memberSet.add(anchorNodeId);
  getUpstream(anchorNodeId, depth, new Set());

  return [...memberSet];
}

// GET all flows
export async function GET() {
  try {
    const dbFlows = getFlows();
    const flows: GraphFlow[] = dbFlows.map((f) => ({
      id: f.id,
      name: f.name,
      description: f.description || undefined,
      anchorNodes: f.anchor_nodes ? JSON.parse(f.anchor_nodes) : [],
      memberNodes: f.member_nodes ? JSON.parse(f.member_nodes) : [],
      userDefined: f.user_defined === 1,
      inferenceReason: f.inference_reason || undefined,
    }));
    return NextResponse.json({ flows });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

// POST create a new flow
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { name, description, anchorNodeIds, naturalLanguage } = body as {
      name: string;
      description?: string;
      anchorNodeIds?: string[];
      naturalLanguage?: string;
    };

    if (!name) {
      return NextResponse.json({ error: "name is required" }, { status: 400 });
    }

    // Get all nodes for traversal
    const dbNodes = getNodes();
    const nodes: GraphNode[] = dbNodes.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      metadata: n.metadata ? (JSON.parse(n.metadata) as NodeMetadata) : undefined,
    }));

    // Get edges for upstream traversal
    const db = getDb();
    const edges = db.prepare("SELECT from_node, to_node FROM edges").all() as Array<{
      from_node: string;
      to_node: string;
    }>;

    // Build upstream map
    const upstreamMap = new Map<string, Set<string>>();
    for (const edge of edges) {
      if (!upstreamMap.has(edge.to_node)) {
        upstreamMap.set(edge.to_node, new Set());
      }
      upstreamMap.get(edge.to_node)!.add(edge.from_node);
    }

    // Find anchor nodes
    let anchors: string[] = [];

    if (anchorNodeIds && anchorNodeIds.length > 0) {
      // Use provided anchor IDs
      anchors = anchorNodeIds.filter((id) => nodes.some((n) => n.id === id));
    } else if (naturalLanguage) {
      // Search for nodes matching the natural language description
      const searchTerms = naturalLanguage.toLowerCase().split(/\s+/);
      const matchingNodes = nodes.filter((n) => {
        const nameMatch = searchTerms.some((term) => n.name.toLowerCase().includes(term));
        const descMatch = n.metadata?.description?.toLowerCase()
          ? searchTerms.some((term) => n.metadata!.description!.toLowerCase().includes(term))
          : false;
        const tagMatch = n.metadata?.tags
          ? searchTerms.some((term) => n.metadata!.tags!.some((t) => t.toLowerCase().includes(term)))
          : false;
        return nameMatch || descMatch || tagMatch;
      });

      // Prefer marts/reports as anchors
      const preferredAnchors = matchingNodes.filter(
        (n) => n.name.startsWith("mart_") || n.name.startsWith("rpt_")
      );
      
      if (preferredAnchors.length > 0) {
        anchors = preferredAnchors.slice(0, 3).map((n) => n.id);
      } else {
        anchors = matchingNodes.slice(0, 3).map((n) => n.id);
      }
    }

    if (anchors.length === 0) {
      return NextResponse.json(
        { error: "Could not find anchor nodes. Try providing specific table names." },
        { status: 400 }
      );
    }

    // Traverse upstream from anchors
    const memberSet = new Set<string>();
    
    function getUpstream(nodeId: string, depth: number, visited: Set<string>): void {
      if (depth === 0 || visited.has(nodeId)) return;
      visited.add(nodeId);
      memberSet.add(nodeId);

      const upstream = upstreamMap.get(nodeId) || new Set();
      for (const upId of upstream) {
        getUpstream(upId, depth - 1, visited);
      }
    }

    for (const anchor of anchors) {
      memberSet.add(anchor);
      getUpstream(anchor, 6, new Set());
    }

    // Create flow
    const flowId = uuid();
    insertFlow({
      id: flowId,
      name,
      description: description || null,
      anchor_nodes: JSON.stringify(anchors),
      member_nodes: JSON.stringify([...memberSet]),
      user_defined: 1,
      inference_reason: naturalLanguage
        ? `User-defined flow from natural language: "${naturalLanguage}"`
        : "User-defined flow with specified anchors",
    });

    return NextResponse.json({
      id: flowId,
      name,
      anchorNodes: anchors,
      memberNodes: [...memberSet],
      memberCount: memberSet.size,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


import { NextRequest, NextResponse } from "next/server";
import { getNodes, getEdges, getGroups, getFlows } from "@/lib/db";
import type { GraphNode, GraphEdge, GraphGroup, GraphFlow, NodeMetadata, EdgeMetadata } from "@/lib/types";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const flowId = searchParams.get("flowId");

  try {
    const dbNodes = getNodes();
    const dbEdges = getEdges();
    const dbGroups = getGroups();
    const dbFlows = getFlows();

    // Convert DB types to API types
    const nodes: GraphNode[] = dbNodes.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      subtype: n.subtype as GraphNode["subtype"],
      groupId: n.group_id || undefined,
      repo: n.repo || undefined,
      metadata: n.metadata ? (JSON.parse(n.metadata) as NodeMetadata) : undefined,
    }));

    const edges: GraphEdge[] = dbEdges.map((e) => ({
      id: e.id,
      from: e.from_node,
      to: e.to_node,
      type: e.type as GraphEdge["type"],
      metadata: e.metadata ? (JSON.parse(e.metadata) as EdgeMetadata) : undefined,
    }));

    const groups: GraphGroup[] = dbGroups.map((g) => ({
      id: g.id,
      name: g.name,
      description: g.description || undefined,
      parentId: g.parent_id || undefined,
      inferenceReason: g.inference_reason || undefined,
      nodeCount: g.node_count,
      collapsedDefault: g.collapsed_default === 1,
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

    // Filter by flow if specified
    let filteredNodes = nodes;
    let filteredEdges = edges;

    if (flowId) {
      const flow = flows.find((f) => f.id === flowId);
      if (flow) {
        const memberSet = new Set(flow.memberNodes);
        filteredNodes = nodes.filter((n) => memberSet.has(n.id));
        filteredEdges = edges.filter(
          (e) => memberSet.has(e.from) && memberSet.has(e.to)
        );
      }
    }

    return NextResponse.json({
      nodes: filteredNodes,
      edges: filteredEdges,
      groups,
      flows,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

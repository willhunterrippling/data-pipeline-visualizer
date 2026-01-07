import { NextRequest, NextResponse } from "next/server";
import {
  getNodeById,
  getCitationsForNode,
  getExplanation,
  getUpstreamNodes,
  getDownstreamNodes,
} from "@/lib/db";
import type { GraphNode, NodeMetadata, Citation } from "@/lib/types";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ nodeId: string }> }
) {
  const { nodeId } = await params;
  const decodedId = decodeURIComponent(nodeId);

  try {
    const dbNode = getNodeById(decodedId);
    if (!dbNode) {
      return NextResponse.json({ error: "Node not found" }, { status: 404 });
    }

    const dbCitations = getCitationsForNode(decodedId);
    const dbExplanation = getExplanation(decodedId);
    const dbUpstream = getUpstreamNodes(decodedId, 3);
    const dbDownstream = getDownstreamNodes(decodedId, 3);

    const node: GraphNode = {
      id: dbNode.id,
      name: dbNode.name,
      type: dbNode.type as GraphNode["type"],
      subtype: dbNode.subtype as GraphNode["subtype"],
      groupId: dbNode.group_id || undefined,
      repo: dbNode.repo || undefined,
      metadata: dbNode.metadata ? (JSON.parse(dbNode.metadata) as NodeMetadata) : undefined,
      layoutX: dbNode.layout_x ?? undefined,
      layoutY: dbNode.layout_y ?? undefined,
      layoutLayer: dbNode.layout_layer ?? undefined,
    };

    const citations: Citation[] = dbCitations.map((c) => ({
      id: c.id,
      nodeId: c.node_id || undefined,
      edgeId: c.edge_id || undefined,
      filePath: c.file_path,
      startLine: c.start_line || undefined,
      endLine: c.end_line || undefined,
      snippet: c.snippet || undefined,
    }));

    const upstream: GraphNode[] = dbUpstream.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      subtype: n.subtype as GraphNode["subtype"],
    }));

    const downstream: GraphNode[] = dbDownstream.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      subtype: n.subtype as GraphNode["subtype"],
    }));

    return NextResponse.json({
      node,
      citations,
      explanation: dbExplanation?.summary || null,
      upstream,
      downstream,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


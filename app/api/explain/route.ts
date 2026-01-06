import { NextRequest, NextResponse } from "next/server";
import { getNodeById, getUpstreamNodes, getDownstreamNodes, getCitationsForNode, insertExplanation, getExplanation } from "@/lib/db";
import { explainNode } from "@/lib/ai/explain";
import type { GraphNode, NodeMetadata, Citation } from "@/lib/types";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { nodeId, forceRefresh } = body as { nodeId: string; forceRefresh?: boolean };

    if (!nodeId) {
      return NextResponse.json({ error: "nodeId is required" }, { status: 400 });
    }

    // Check for cached explanation
    if (!forceRefresh) {
      const cached = getExplanation(nodeId);
      if (cached?.summary) {
        return NextResponse.json({
          explanation: cached.summary,
          cached: true,
          generatedAt: cached.generated_at,
        });
      }
    }

    // Get node data
    const dbNode = getNodeById(nodeId);
    if (!dbNode) {
      return NextResponse.json({ error: "Node not found" }, { status: 404 });
    }

    // Convert to GraphNode
    const node: GraphNode = {
      id: dbNode.id,
      name: dbNode.name,
      type: dbNode.type as GraphNode["type"],
      subtype: dbNode.subtype as GraphNode["subtype"],
      repo: dbNode.repo || undefined,
      metadata: dbNode.metadata ? (JSON.parse(dbNode.metadata) as NodeMetadata) : undefined,
    };

    // Get upstream/downstream
    const dbUpstream = getUpstreamNodes(nodeId, 2);
    const dbDownstream = getDownstreamNodes(nodeId, 2);
    const dbCitations = getCitationsForNode(nodeId);

    const upstream: GraphNode[] = dbUpstream.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
    }));

    const downstream: GraphNode[] = dbDownstream.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
    }));

    const citations: Citation[] = dbCitations.map((c) => ({
      id: c.id,
      nodeId: c.node_id || undefined,
      filePath: c.file_path,
      startLine: c.start_line || undefined,
      endLine: c.end_line || undefined,
    }));

    // Generate explanation
    const explanation = await explainNode(node, {
      upstream,
      downstream,
      citations,
      repoPath: process.env.RIPPLING_DBT_PATH,
    });

    // Cache the explanation
    insertExplanation({
      node_id: nodeId,
      summary: explanation,
      generated_at: new Date().toISOString(),
      model_used: process.env.OPENAI_MODEL || "o1",
    });

    return NextResponse.json({
      explanation,
      cached: false,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


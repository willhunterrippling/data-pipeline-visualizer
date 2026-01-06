import { NextRequest, NextResponse } from "next/server";
import { searchNodes, getNodes } from "@/lib/db";
import type { GraphNode, NodeMetadata } from "@/lib/types";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const query = searchParams.get("q");
  const limit = parseInt(searchParams.get("limit") || "50", 10);

  try {
    let dbNodes;

    if (query && query.length >= 2) {
      // Use FTS search
      dbNodes = searchNodes(`${query}*`, limit);
    } else {
      // Return all nodes if no query
      dbNodes = getNodes().slice(0, limit);
    }

    const nodes: GraphNode[] = dbNodes.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      subtype: n.subtype as GraphNode["subtype"],
      groupId: n.group_id || undefined,
      repo: n.repo || undefined,
      metadata: n.metadata ? (JSON.parse(n.metadata) as NodeMetadata) : undefined,
    }));

    return NextResponse.json({ nodes, total: nodes.length });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


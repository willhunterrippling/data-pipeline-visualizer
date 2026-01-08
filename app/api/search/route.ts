import { NextRequest, NextResponse } from "next/server";
import { searchNodes, getNodes, getDb, DbNode } from "@/lib/db";
import type { GraphNode, NodeMetadata } from "@/lib/types";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const query = searchParams.get("q");
  const limit = parseInt(searchParams.get("limit") || "50", 10);

  try {
    let dbNodes: DbNode[];
    const db = getDb();

    if (query && query.length >= 2) {
      // Use FTS search for main results
      dbNodes = searchNodes(`${query}*`, limit);
      
      // FTS5 has issues matching external nodes (case sensitivity, tokenization)
      // Supplement with direct LIKE search on external nodes to ensure they appear
      const existingIds = new Set(dbNodes.map(n => n.id));
      const externalMatches = db.prepare(`
        SELECT * FROM nodes 
        WHERE type = 'external' 
        AND (name LIKE ? OR id LIKE ?)
        LIMIT 10
      `).all(`%${query}%`, `%${query}%`) as DbNode[];
      
      // Add external matches that weren't already found
      for (const ext of externalMatches) {
        if (!existingIds.has(ext.id)) {
          dbNodes.push(ext);
        }
      }
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


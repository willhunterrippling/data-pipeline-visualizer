import { NextRequest, NextResponse } from "next/server";
import { searchNodes, getNodes, isStaticMode, DbNode } from "@/lib/db";
import type { GraphNode, NodeMetadata } from "@/lib/types";

// Only import getDb when not in static mode
let getDb: (() => ReturnType<typeof import("@/lib/db").getDb>) | null = null;
if (!isStaticMode()) {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  getDb = require("@/lib/db").getDb;
}

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const query = searchParams.get("q");
  const limit = parseInt(searchParams.get("limit") || "50", 10);

  try {
    let dbNodes: DbNode[];

    if (query && query.length >= 2) {
      // Use FTS search for main results (works in both modes)
      dbNodes = searchNodes(`${query}*`, limit);
      
      // FTS5 has issues matching external nodes (case sensitivity, tokenization)
      // Supplement with additional search on external nodes to ensure they appear
      const existingIds = new Set(dbNodes.map(n => n.id));
      const queryLower = query.toLowerCase();
      
      if (isStaticMode() || !getDb) {
        // In static mode, search external nodes in memory
        const allNodes = getNodes();
        for (const node of allNodes) {
          if (node.type === 'external' && !existingIds.has(node.id)) {
            if (node.name.toLowerCase().includes(queryLower) || 
                node.id.toLowerCase().includes(queryLower)) {
              dbNodes.push(node);
              existingIds.add(node.id);
              if (dbNodes.length >= limit + 10) break;
            }
          }
        }
      } else {
        // SQLite mode - use LIKE query
        const db = getDb();
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


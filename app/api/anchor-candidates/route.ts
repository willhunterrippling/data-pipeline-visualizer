import { NextRequest, NextResponse } from "next/server";
import { getAnchorCandidates, getNodeById, getDb } from "@/lib/db";

interface EnrichedCandidate {
  nodeId: string;
  nodeName: string;
  nodeType: string;
  nodeSubtype?: string;
  importanceScore: number;
  upstreamCount: number;
  downstreamCount: number;
  totalConnections: number;
  reason: string | null;
}

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const limit = parseInt(searchParams.get("limit") || "20");
  const typeFilter = searchParams.get("type"); // Filter by node type (e.g., "external")
  const includeExternal = searchParams.get("includeExternal") === "true";

  try {
    const candidates = getAnchorCandidates(limit * 2); // Get more to filter
    const db = getDb();

    // Enrich with node names and apply type filtering
    let enrichedCandidates: EnrichedCandidate[] = candidates.map((c) => {
      const node = getNodeById(c.node_id);
      return {
        nodeId: c.node_id,
        nodeName: node?.name || c.node_id,
        nodeType: node?.type || "unknown",
        nodeSubtype: node?.subtype || undefined,
        importanceScore: c.importance_score,
        upstreamCount: c.upstream_count,
        downstreamCount: c.downstream_count,
        totalConnections: c.total_connections,
        reason: c.reason,
      };
    });

    // Filter by type if specified
    if (typeFilter) {
      enrichedCandidates = enrichedCandidates.filter(
        (c) => c.nodeType === typeFilter
      );
    }

    // If includeExternal is true, also fetch external nodes that might not be in candidates
    if (includeExternal && !typeFilter) {
      const externalNodes = db
        .prepare(
          `SELECT n.id, n.name, n.type, n.subtype, n.importance_score,
                  (SELECT COUNT(*) FROM edges WHERE to_node = n.id) as upstream_count,
                  (SELECT COUNT(*) FROM edges WHERE from_node = n.id) as downstream_count
           FROM nodes n 
           WHERE n.type = 'external'
           ORDER BY n.importance_score DESC NULLS LAST
           LIMIT ?`
        )
        .all(limit) as Array<{
          id: string;
          name: string;
          type: string;
          subtype: string | null;
          importance_score: number | null;
          upstream_count: number;
          downstream_count: number;
        }>;

      // Add external nodes that aren't already in the candidates
      const existingIds = new Set(enrichedCandidates.map((c) => c.nodeId));
      for (const ext of externalNodes) {
        if (!existingIds.has(ext.id)) {
          enrichedCandidates.push({
            nodeId: ext.id,
            nodeName: ext.name,
            nodeType: ext.type,
            nodeSubtype: ext.subtype || undefined,
            importanceScore: ext.importance_score || 0.15,
            upstreamCount: ext.upstream_count,
            downstreamCount: ext.downstream_count,
            totalConnections: ext.upstream_count + ext.downstream_count,
            reason: "External system",
          });
        }
      }

      // Re-sort by importance score
      enrichedCandidates.sort((a, b) => b.importanceScore - a.importanceScore);
    }

    // Apply final limit
    enrichedCandidates = enrichedCandidates.slice(0, limit);

    return NextResponse.json({
      candidates: enrichedCandidates,
      total: enrichedCandidates.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


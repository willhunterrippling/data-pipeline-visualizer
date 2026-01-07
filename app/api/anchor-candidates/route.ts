import { NextRequest, NextResponse } from "next/server";
import { getAnchorCandidates, getNodeById } from "@/lib/db";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const limit = parseInt(searchParams.get("limit") || "20");

  try {
    const candidates = getAnchorCandidates(limit);

    // Enrich with node names
    const enrichedCandidates = candidates.map((c) => {
      const node = getNodeById(c.node_id);
      return {
        nodeId: c.node_id,
        nodeName: node?.name || c.node_id,
        nodeType: node?.type || "unknown",
        importanceScore: c.importance_score,
        upstreamCount: c.upstream_count,
        downstreamCount: c.downstream_count,
        totalConnections: c.total_connections,
        reason: c.reason,
      };
    });

    return NextResponse.json({
      candidates: enrichedCandidates,
      total: enrichedCandidates.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}


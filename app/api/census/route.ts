import { NextRequest, NextResponse } from "next/server";
import { v4 as uuid } from "uuid";
import {
  getDb,
  isStaticMode,
  getNodes,
  getEdges,
  insertNode,
  insertEdges,
  insertCitations,
  clearLineageCache,
  DbNode,
  DbEdge,
  DbCitation,
} from "@/lib/db";
import {
  parseCensusConfig,
  normalizeCensusResponse,
  validateCensusConfig,
} from "@/lib/indexer/censusParser";
import type { GraphNode } from "@/lib/types";

/**
 * POST /api/census
 * 
 * Upload Census sync configuration JSON to create reverse ETL edges.
 * This captures "loop-back" patterns where mart models feed external pipelines
 * that write back to Snowflake tables consumed by staging models.
 * 
 * Request body: Census API response JSON (from /api/v1/syncs)
 * 
 * Example:
 * curl -X POST http://localhost:3000/api/census \
 *   -H "Content-Type: application/json" \
 *   -d @census-syncs.json
 */
export async function POST(request: NextRequest) {
  // Disable in production (static mode)
  if (isStaticMode()) {
    return NextResponse.json(
      { error: "Census import is not available in production. Run locally with npm run dev." },
      { status: 405 }
    );
  }

  try {
    // Parse request body
    const body = await request.json();

    // Validate Census config structure
    const validation = validateCensusConfig(body);
    if (!validation.valid) {
      return NextResponse.json(
        { error: `Invalid Census config: ${validation.error}` },
        { status: 400 }
      );
    }

    // Normalize the response format
    const config = normalizeCensusResponse(body);

    // Initialize database and get existing nodes
    getDb();
    const dbNodes = getNodes();
    const existingEdges = getEdges();

    // Convert DB nodes to GraphNodes for the parser
    const graphNodes: GraphNode[] = dbNodes.map((n: DbNode) => ({
      id: n.id,
      name: n.name,
      type: n.type as GraphNode["type"],
      subtype: n.subtype as GraphNode["subtype"],
      repo: n.repo || undefined,
      metadata: n.metadata ? JSON.parse(n.metadata) : undefined,
    }));

    // Parse Census config
    const result = parseCensusConfig(config, graphNodes);

    // Track what we're adding
    const existingEdgeKeys = new Set(existingEdges.map((e: DbEdge) => `${e.from_node}|${e.to_node}`));
    const existingNodeIds = new Set(dbNodes.map((n: DbNode) => n.id));

    // Add Census node if it doesn't exist
    let censusNodeAdded = false;
    for (const node of result.nodes) {
      if (!existingNodeIds.has(node.id)) {
        const dbNode: Omit<DbNode, "created_at"> = {
          id: node.id,
          name: node.name,
          type: node.type,
          subtype: node.subtype || null,
          group_id: null,
          repo: node.repo || null,
          metadata: node.metadata ? JSON.stringify(node.metadata) : null,
          sql_content: null,
          layout_x: null,
          layout_y: null,
          layout_layer: null,
          semantic_layer: node.semanticLayer || null,
          importance_score: null,
        };
        insertNode(dbNode);
        existingNodeIds.add(node.id);
        censusNodeAdded = true;
      }
    }

    // Filter out edges that already exist
    const newEdges: DbEdge[] = [];
    for (const edge of result.edges) {
      const key = `${edge.from}|${edge.to}`;
      if (!existingEdgeKeys.has(key)) {
        newEdges.push({
          id: edge.id,
          from_node: edge.from,
          to_node: edge.to,
          type: edge.type,
          metadata: edge.metadata ? JSON.stringify(edge.metadata) : null,
        });
        existingEdgeKeys.add(key);
      }
    }

    // Insert new edges
    if (newEdges.length > 0) {
      insertEdges(newEdges);
    }

    // Insert citations
    if (result.citations.length > 0) {
      const dbCitations: DbCitation[] = result.citations.map(c => ({
        id: c.id,
        node_id: c.nodeId || null,
        edge_id: c.edgeId || null,
        file_path: c.filePath,
        start_line: c.startLine || null,
        end_line: c.endLine || null,
        snippet: c.snippet || null,
      }));
      insertCitations(dbCitations);
    }

    // Clear lineage cache since graph structure changed
    if (newEdges.length > 0) {
      clearLineageCache();
    }

    return NextResponse.json({
      success: true,
      stats: {
        syncsProcessed: result.stats.syncsProcessed,
        matchedSources: result.stats.matchedSources.length,
        edgesCreated: newEdges.length,
        loopBacksDetected: result.stats.loopBacksDetected,
        censusNodeCreated: censusNodeAdded,
        matchedSourceDetails: result.stats.matchedSources.slice(0, 10), // First 10 for quick view
        unmatchedSources: result.stats.unmatchedSources,
        unmatchedDestinations: result.stats.unmatchedDestinations,
      },
      message: newEdges.length > 0
        ? `Created ${newEdges.length} reverse ETL edges from ${result.stats.syncsProcessed} Census syncs (${result.stats.matchedSources.length} matched)`
        : `No new edges created (${result.stats.syncsProcessed} syncs processed, ${result.stats.matchedSources.length} matched, ${result.stats.unmatchedSources.length} unmatched)`,
    });

  } catch (error) {
    console.error("Census import error:", error);
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json(
      { error: `Failed to import Census config: ${message}` },
      { status: 500 }
    );
  }
}

/**
 * GET /api/census
 * 
 * Get instructions for importing Census config.
 */
export async function GET() {
  return NextResponse.json({
    description: "Census Reverse ETL Import",
    autoLoadBehavior: "The indexer automatically loads Census data from data/census.json if present. If the file is missing or empty, Census integration is skipped with a warning.",
    instructions: {
      recommended: [
        "1. Use the Python export script (requires API key):",
        "   python scripts/export_census_data.py --api-key YOUR_API_KEY",
        "2. The script saves to data/census.json which is auto-loaded during indexing",
        "3. Commit data/census.json to your repo for team sharing",
      ],
      alternative: [
        "1. Get your Census API token from https://app.getcensus.com/settings/api",
        "2. Fetch your syncs: curl -H 'Authorization: Bearer YOUR_TOKEN' https://app.getcensus.com/api/v1/syncs > census-syncs.json",
        "3. Fetch connections: curl -H 'Authorization: Bearer YOUR_TOKEN' https://app.getcensus.com/api/v1/connections > census-connections.json",
        "4. Merge the files or use the syncs file directly",
        "5. Either: save to data/census.json for auto-loading, or POST to this endpoint for one-time import",
      ],
    },
    expectedFormat: {
      syncs: [
        {
          id: 123,
          label: "Example Sync",
          source: {
            connection_id: 1,
            object: "mart_growth__lsw_lead_data",
          },
          destination: {
            connection_id: 2,
            object: "GROWTH.MECHANIZED_OUTREACH_POPULATION",
          },
        },
      ],
      connections: [
        {
          id: 1,
          name: "Snowflake DWH",
          type: "snowflake",
        },
        {
          id: 2,
          name: "Snowflake DWH",
          type: "snowflake",
        },
      ],
    },
  });
}

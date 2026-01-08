import { NextRequest, NextResponse } from "next/server";
import {
  getNodeById,
  getRelationalExplanation,
  insertRelationalExplanation,
  getDb,
  DbNode,
} from "@/lib/db";
import { explainRelationship, PathInfo } from "@/lib/ai/relationalExplain";
import type { GraphNode, NodeMetadata } from "@/lib/types";

/**
 * Find the path between two nodes in the graph.
 * Returns direction (upstream/downstream), hop count, and intermediate nodes.
 */
function findPath(nodeId: string, anchorId: string): PathInfo | null {
  const db = getDb();

  // First, try to find path from node to anchor (node is upstream of anchor)
  const upstreamPath = db.prepare(`
    WITH RECURSIVE path AS (
      -- Base case: direct edges from node
      SELECT 
        from_node as current,
        to_node as next,
        1 as depth,
        from_node || ',' || to_node as path_nodes
      FROM edges 
      WHERE from_node = ?
      
      UNION ALL
      
      -- Recursive case: follow edges
      SELECT 
        e.from_node,
        e.to_node,
        p.depth + 1,
        p.path_nodes || ',' || e.to_node
      FROM edges e
      JOIN path p ON e.from_node = p.next
      WHERE p.depth < 10  -- Max depth to prevent infinite loops
        AND p.path_nodes NOT LIKE '%' || e.to_node || '%'  -- Prevent cycles
    )
    SELECT depth, path_nodes
    FROM path
    WHERE next = ?
    ORDER BY depth ASC
    LIMIT 1
  `).get(nodeId, anchorId) as { depth: number; path_nodes: string } | undefined;

  if (upstreamPath) {
    // Node is upstream of anchor
    const pathNodeIds = upstreamPath.path_nodes.split(",");
    // Remove source and target, keep intermediates
    const intermediateIds = pathNodeIds.slice(1, -1);
    const intermediateNodes = intermediateIds
      .map((id) => getNodeById(id))
      .filter((n): n is DbNode => n !== undefined)
      .map(dbNodeToGraphNode);

    return {
      direction: "upstream",
      hops: upstreamPath.depth,
      intermediateNodes,
    };
  }

  // Try to find path from anchor to node (node is downstream of anchor)
  const downstreamPath = db.prepare(`
    WITH RECURSIVE path AS (
      -- Base case: direct edges from anchor
      SELECT 
        from_node as current,
        to_node as next,
        1 as depth,
        from_node || ',' || to_node as path_nodes
      FROM edges 
      WHERE from_node = ?
      
      UNION ALL
      
      -- Recursive case: follow edges
      SELECT 
        e.from_node,
        e.to_node,
        p.depth + 1,
        p.path_nodes || ',' || e.to_node
      FROM edges e
      JOIN path p ON e.from_node = p.next
      WHERE p.depth < 10  -- Max depth to prevent infinite loops
        AND p.path_nodes NOT LIKE '%' || e.to_node || '%'  -- Prevent cycles
    )
    SELECT depth, path_nodes
    FROM path
    WHERE next = ?
    ORDER BY depth ASC
    LIMIT 1
  `).get(anchorId, nodeId) as { depth: number; path_nodes: string } | undefined;

  if (downstreamPath) {
    // Node is downstream of anchor
    const pathNodeIds = downstreamPath.path_nodes.split(",");
    // Remove source and target, keep intermediates
    const intermediateIds = pathNodeIds.slice(1, -1);
    const intermediateNodes = intermediateIds
      .map((id) => getNodeById(id))
      .filter((n): n is DbNode => n !== undefined)
      .map(dbNodeToGraphNode);

    return {
      direction: "downstream",
      hops: downstreamPath.depth,
      intermediateNodes,
    };
  }

  // No path found
  return null;
}

function dbNodeToGraphNode(dbNode: DbNode): GraphNode {
  return {
    id: dbNode.id,
    name: dbNode.name,
    type: dbNode.type as GraphNode["type"],
    subtype: dbNode.subtype as GraphNode["subtype"],
    repo: dbNode.repo || undefined,
    metadata: dbNode.metadata ? (JSON.parse(dbNode.metadata) as NodeMetadata) : undefined,
  };
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { nodeId, anchorId, forceRefresh } = body as {
      nodeId: string;
      anchorId: string;
      forceRefresh?: boolean;
    };

    if (!nodeId || !anchorId) {
      return NextResponse.json(
        { error: "nodeId and anchorId are required" },
        { status: 400 }
      );
    }

    if (nodeId === anchorId) {
      return NextResponse.json(
        { error: "nodeId and anchorId cannot be the same" },
        { status: 400 }
      );
    }

    // Check for cached explanation
    if (!forceRefresh) {
      const cached = getRelationalExplanation(nodeId, anchorId);
      if (cached?.full_explanation) {
        return NextResponse.json({
          explanation: cached.full_explanation,
          transformationSummary: cached.transformation_summary,
          businessContext: cached.business_context,
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

    const dbAnchor = getNodeById(anchorId);
    if (!dbAnchor) {
      return NextResponse.json({ error: "Anchor not found" }, { status: 404 });
    }

    // Convert to GraphNode
    const node = dbNodeToGraphNode(dbNode);
    const anchor = dbNodeToGraphNode(dbAnchor);

    // Find path between nodes
    const pathInfo = findPath(nodeId, anchorId);

    if (!pathInfo) {
      // No path found - nodes are not connected
      return NextResponse.json({
        explanation: `\`${node.name}\` and \`${anchor.name}\` are not directly connected in the data lineage graph.`,
        transformationSummary: null,
        businessContext: null,
        cached: false,
        generatedAt: new Date().toISOString(),
        noPath: true,
      });
    }

    // Generate explanation with SQL context
    const result = await explainRelationship(node, anchor, pathInfo, {
      repoPath: process.env.RIPPLING_DBT_PATH,
    });

    // Cache the explanation
    insertRelationalExplanation({
      node_id: nodeId,
      anchor_id: anchorId,
      transformation_summary: result.transformationSummary,
      business_context: result.businessContext,
      full_explanation: result.fullExplanation,
      generated_at: new Date().toISOString(),
      model_used: process.env.OPENAI_MODEL || "gpt-4.1",
    });

    return NextResponse.json({
      explanation: result.fullExplanation,
      transformationSummary: result.transformationSummary,
      businessContext: result.businessContext,
      cached: false,
      generatedAt: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Failed to generate relational explanation:", error);
    const message = error instanceof Error ? error.message : "Unknown error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

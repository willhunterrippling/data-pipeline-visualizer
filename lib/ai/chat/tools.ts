/**
 * Agent tools for the Pipeline Chat feature.
 * These tools allow the AI agent to query the graph database to gather context.
 */

import {
  getDb,
  getNodeById,
  getUpstreamNodes,
  getDownstreamNodes,
  getFlows,
  getExplanation,
  getCitationsForNode,
  type DbNode,
  type DbFlow,
} from "@/lib/db";
import type { GraphNode, GraphFlow, ColumnInfo } from "@/lib/types";

// ============================================================================
// Tool Result Types
// ============================================================================

export interface NodeSummary {
  id: string;
  name: string;
  type: string;
  subtype?: string;
  description?: string;
  schema?: string;
  database?: string;
  semanticLayer?: string;
  tags?: string[];
}

export interface NodeDetails extends NodeSummary {
  columns?: ColumnInfo[];
  materialization?: string;
  sql?: string;
  upstreamCount: number;
  downstreamCount: number;
  explanation?: string;
}

export interface LineageResult {
  nodes: NodeSummary[];
  totalCount: number;
  depth: number;
}

export interface FlowSummary {
  id: string;
  name: string;
  description?: string;
  anchorCount: number;
  memberCount: number;
}

export interface FlowDetails extends FlowSummary {
  anchorNodes: string[];
  memberNodes: string[];
  inferenceReason?: string;
}

export interface ColumnSearchResult {
  nodeId: string;
  nodeName: string;
  columnName: string;
  columnType: string;
  columnDescription?: string;
}

export interface SqlSearchResult {
  nodeId: string;
  nodeName: string;
  nodeType: string;
  semanticLayer?: string;
  matchSnippet: string; // Snippet of SQL around the match
}

// ============================================================================
// Helper Functions
// ============================================================================

function dbNodeToSummary(dbNode: DbNode): NodeSummary {
  const metadata = dbNode.metadata ? JSON.parse(dbNode.metadata) : {};
  return {
    id: dbNode.id,
    name: dbNode.name,
    type: dbNode.type,
    subtype: dbNode.subtype || undefined,
    description: metadata.description,
    schema: metadata.schema,
    database: metadata.database,
    semanticLayer: dbNode.semantic_layer || undefined,
    tags: metadata.tags,
  };
}

function dbFlowToSummary(dbFlow: DbFlow): FlowSummary {
  const anchorNodes: string[] = dbFlow.anchor_nodes ? JSON.parse(dbFlow.anchor_nodes) : [];
  const memberNodes: string[] = dbFlow.member_nodes ? JSON.parse(dbFlow.member_nodes) : [];
  return {
    id: dbFlow.id,
    name: dbFlow.name,
    description: dbFlow.description || undefined,
    anchorCount: anchorNodes.length,
    memberCount: memberNodes.length,
  };
}

// ============================================================================
// Tool Implementations
// ============================================================================

/**
 * Search for nodes by name, description, or metadata.
 * Uses FTS (full-text search) for efficient querying.
 */
export function searchNodes(
  query: string,
  options?: {
    type?: string;
    limit?: number;
  }
): NodeSummary[] {
  const db = getDb();
  const limit = options?.limit ?? 20;

  // Clean query for FTS - escape special characters and add wildcards
  const cleanQuery = query
    .replace(/[^\w\s]/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((term) => `${term}*`)
    .join(" ");

  if (!cleanQuery) {
    return [];
  }

  let sql = `
    SELECT DISTINCT nodes.* FROM nodes_fts
    JOIN nodes ON nodes_fts.id = nodes.id
    WHERE nodes_fts MATCH ?
  `;
  const params: (string | number)[] = [cleanQuery];

  if (options?.type) {
    sql += ` AND nodes.type = ?`;
    params.push(options.type);
  }

  sql += ` LIMIT ?`;
  params.push(limit);

  try {
    const results = db.prepare(sql).all(...params) as DbNode[];
    return results.map(dbNodeToSummary);
  } catch {
    // FTS query failed, fall back to LIKE search
    const likeQuery = `%${query}%`;
    let fallbackSql = `SELECT * FROM nodes WHERE (name LIKE ? OR id LIKE ?)`;
    const fallbackParams: (string | number)[] = [likeQuery, likeQuery];

    if (options?.type) {
      fallbackSql += ` AND type = ?`;
      fallbackParams.push(options.type);
    }

    fallbackSql += ` LIMIT ?`;
    fallbackParams.push(limit);

    const results = db.prepare(fallbackSql).all(...fallbackParams) as DbNode[];
    return results.map(dbNodeToSummary);
  }
}

/**
 * Get detailed information about a specific node, including SQL and columns.
 */
export function getNodeDetails(nodeId: string): NodeDetails | null {
  const dbNode = getNodeById(nodeId);
  if (!dbNode) {
    return null;
  }

  const metadata = dbNode.metadata ? JSON.parse(dbNode.metadata) : {};
  const explanation = getExplanation(nodeId);

  // Get SQL content from database (stored during ingestion)
  let sql: string | undefined;
  if (dbNode.sql_content) {
    sql = dbNode.sql_content;
  }

  // Get upstream/downstream counts
  const db = getDb();
  const upstreamCount = (
    db.prepare("SELECT COUNT(*) as count FROM edges WHERE to_node = ?").get(nodeId) as { count: number }
  ).count;
  const downstreamCount = (
    db.prepare("SELECT COUNT(*) as count FROM edges WHERE from_node = ?").get(nodeId) as { count: number }
  ).count;

  return {
    id: dbNode.id,
    name: dbNode.name,
    type: dbNode.type,
    subtype: dbNode.subtype || undefined,
    description: metadata.description,
    schema: metadata.schema,
    database: metadata.database,
    semanticLayer: dbNode.semantic_layer || undefined,
    tags: metadata.tags,
    columns: metadata.columns,
    materialization: metadata.materialization,
    sql: sql ? (sql.length > 3000 ? sql.substring(0, 3000) + "\n... (truncated)" : sql) : undefined,
    upstreamCount,
    downstreamCount,
    explanation: explanation?.summary || undefined,
  };
}

/**
 * Get upstream lineage for a node.
 */
export function getUpstreamLineage(
  nodeId: string,
  depth: number = 3
): LineageResult {
  const maxDepth = Math.min(depth, 6); // Cap at 6 to prevent huge results
  const nodes = getUpstreamNodes(nodeId, maxDepth);

  return {
    nodes: nodes.map(dbNodeToSummary),
    totalCount: nodes.length,
    depth: maxDepth,
  };
}

/**
 * Get downstream lineage for a node.
 */
export function getDownstreamLineage(
  nodeId: string,
  depth: number = 3
): LineageResult {
  const maxDepth = Math.min(depth, 6); // Cap at 6 to prevent huge results
  const nodes = getDownstreamNodes(nodeId, maxDepth);

  return {
    nodes: nodes.map(dbNodeToSummary),
    totalCount: nodes.length,
    depth: maxDepth,
  };
}

/**
 * Find flows that contain a specific node.
 */
export function findFlowsContaining(nodeId: string): FlowSummary[] {
  const allFlows = getFlows();
  const matchingFlows: FlowSummary[] = [];

  for (const flow of allFlows) {
    const memberNodes: string[] = flow.member_nodes ? JSON.parse(flow.member_nodes) : [];
    const anchorNodes: string[] = flow.anchor_nodes ? JSON.parse(flow.anchor_nodes) : [];

    if (memberNodes.includes(nodeId) || anchorNodes.includes(nodeId)) {
      matchingFlows.push(dbFlowToSummary(flow));
    }
  }

  return matchingFlows;
}

/**
 * Get detailed information about a specific flow.
 */
export function getFlowDetails(flowId: string): FlowDetails | null {
  const allFlows = getFlows();
  const flow = allFlows.find((f) => f.id === flowId);

  if (!flow) {
    return null;
  }

  const anchorNodes: string[] = flow.anchor_nodes ? JSON.parse(flow.anchor_nodes) : [];
  const memberNodes: string[] = flow.member_nodes ? JSON.parse(flow.member_nodes) : [];

  return {
    id: flow.id,
    name: flow.name,
    description: flow.description || undefined,
    anchorCount: anchorNodes.length,
    memberCount: memberNodes.length,
    anchorNodes,
    memberNodes,
    inferenceReason: flow.inference_reason || undefined,
  };
}

/**
 * List all available flows.
 */
export function listFlows(): FlowSummary[] {
  const allFlows = getFlows();
  return allFlows.map(dbFlowToSummary);
}

/**
 * Search for nodes that have a specific column.
 */
export function searchByColumn(
  columnName: string,
  options?: {
    limit?: number;
  }
): ColumnSearchResult[] {
  const db = getDb();
  const limit = options?.limit ?? 20;

  // Search in metadata JSON for column names
  const searchTerm = `%"name":"${columnName}%`;
  const results = db
    .prepare(
      `SELECT * FROM nodes WHERE metadata LIKE ? LIMIT ?`
    )
    .all(searchTerm, limit * 2) as DbNode[]; // Get more to filter

  const columnResults: ColumnSearchResult[] = [];

  for (const node of results) {
    if (!node.metadata) continue;

    try {
      const metadata = JSON.parse(node.metadata);
      const columns = metadata.columns as ColumnInfo[] | undefined;

      if (columns) {
        for (const col of columns) {
          if (col.name.toLowerCase().includes(columnName.toLowerCase())) {
            columnResults.push({
              nodeId: node.id,
              nodeName: node.name,
              columnName: col.name,
              columnType: col.type,
              columnDescription: col.description,
            });

            if (columnResults.length >= limit) {
              return columnResults;
            }
          }
        }
      }
    } catch {
      // Skip invalid JSON
    }
  }

  return columnResults;
}

/**
 * Search within SQL content to find models that implement specific logic.
 * Uses FTS on sql_content column for efficient pattern matching.
 */
export function searchSqlContent(
  pattern: string,
  options?: {
    limit?: number;
  }
): SqlSearchResult[] {
  const db = getDb();
  const limit = options?.limit ?? 10;

  // Clean pattern for FTS - escape special characters and add wildcards
  const cleanPattern = pattern
    .replace(/[^\w\s]/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((term) => `${term}*`)
    .join(" ");

  if (!cleanPattern) {
    return [];
  }

  const results: SqlSearchResult[] = [];

  try {
    // Use FTS to search in sql_content
    const rows = db.prepare(`
      SELECT DISTINCT nodes.* FROM nodes_fts
      JOIN nodes ON nodes_fts.id = nodes.id
      WHERE nodes_fts.sql_content MATCH ?
      AND nodes.sql_content IS NOT NULL
      LIMIT ?
    `).all(cleanPattern, limit * 2) as DbNode[]; // Get extra to account for potential filtering

    for (const row of rows) {
      if (!row.sql_content) continue;
      
      // Extract a snippet around the match
      const sql = row.sql_content;
      const patternLower = pattern.toLowerCase();
      const sqlLower = sql.toLowerCase();
      const matchIndex = sqlLower.indexOf(patternLower);
      
      let snippet: string;
      if (matchIndex >= 0) {
        // Show context around the match
        const start = Math.max(0, matchIndex - 50);
        const end = Math.min(sql.length, matchIndex + pattern.length + 100);
        snippet = (start > 0 ? "..." : "") + sql.substring(start, end).trim() + (end < sql.length ? "..." : "");
      } else {
        // If exact match not found (due to FTS tokenization), show start of SQL
        snippet = sql.substring(0, 150).trim() + (sql.length > 150 ? "..." : "");
      }

      results.push({
        nodeId: row.id,
        nodeName: row.name,
        nodeType: row.type,
        semanticLayer: row.semantic_layer || undefined,
        matchSnippet: snippet,
      });

      if (results.length >= limit) break;
    }
  } catch {
    // FTS query failed, fall back to LIKE search
    const likePattern = `%${pattern}%`;
    const rows = db.prepare(`
      SELECT * FROM nodes 
      WHERE sql_content LIKE ? 
      LIMIT ?
    `).all(likePattern, limit) as DbNode[];

    for (const row of rows) {
      if (!row.sql_content) continue;
      
      const sql = row.sql_content;
      const patternLower = pattern.toLowerCase();
      const sqlLower = sql.toLowerCase();
      const matchIndex = sqlLower.indexOf(patternLower);
      
      let snippet: string;
      if (matchIndex >= 0) {
        const start = Math.max(0, matchIndex - 50);
        const end = Math.min(sql.length, matchIndex + pattern.length + 100);
        snippet = (start > 0 ? "..." : "") + sql.substring(start, end).trim() + (end < sql.length ? "..." : "");
      } else {
        snippet = sql.substring(0, 150).trim() + (sql.length > 150 ? "..." : "");
      }

      results.push({
        nodeId: row.id,
        nodeName: row.name,
        nodeType: row.type,
        semanticLayer: row.semantic_layer || undefined,
        matchSnippet: snippet,
      });
    }
  }

  return results;
}

/**
 * Get graph statistics for context.
 */
export function getGraphStats(): {
  totalNodes: number;
  totalEdges: number;
  totalFlows: number;
  nodesByType: Record<string, number>;
} {
  const db = getDb();

  const totalNodes = (db.prepare("SELECT COUNT(*) as count FROM nodes").get() as { count: number }).count;
  const totalEdges = (db.prepare("SELECT COUNT(*) as count FROM edges").get() as { count: number }).count;
  const totalFlows = (db.prepare("SELECT COUNT(*) as count FROM flows").get() as { count: number }).count;

  const typeResults = db
    .prepare("SELECT type, COUNT(*) as count FROM nodes GROUP BY type")
    .all() as Array<{ type: string; count: number }>;

  const nodesByType: Record<string, number> = {};
  for (const row of typeResults) {
    nodesByType[row.type] = row.count;
  }

  return {
    totalNodes,
    totalEdges,
    totalFlows,
    nodesByType,
  };
}

// ============================================================================
// OpenAI Function Definitions (for function calling)
// ============================================================================

export const TOOL_DEFINITIONS = [
  {
    type: "function" as const,
    function: {
      name: "searchNodes",
      description:
        "Search for tables/models in the data pipeline by name, description, or keywords. Returns a list of matching nodes with basic info.",
      parameters: {
        type: "object",
        properties: {
          query: {
            type: "string",
            description: "Search query - can be table name, partial name, or keywords",
          },
          type: {
            type: "string",
            enum: ["table", "view", "model", "source", "seed", "external"],
            description: "Optional filter by node type",
          },
          limit: {
            type: "number",
            description: "Maximum results to return (default 20)",
          },
        },
        required: ["query"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "getNodeDetails",
      description:
        "Get detailed information about a specific node including its SQL code, columns, and existing explanation. Use this after finding a node via search.",
      parameters: {
        type: "object",
        properties: {
          nodeId: {
            type: "string",
            description: "The full node ID (e.g., GROWTH.RAW.LEADS or model.rippling_dbt.stg_sfdc__leads)",
          },
        },
        required: ["nodeId"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "getUpstreamLineage",
      description:
        "Get the upstream data sources that feed into a node. Use this to trace where data comes from.",
      parameters: {
        type: "object",
        properties: {
          nodeId: {
            type: "string",
            description: "The node ID to get upstream lineage for",
          },
          depth: {
            type: "number",
            description: "How many levels upstream to traverse (default 3, max 6)",
          },
        },
        required: ["nodeId"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "getDownstreamLineage",
      description:
        "Get the downstream consumers of a node. Use this to see what depends on this data.",
      parameters: {
        type: "object",
        properties: {
          nodeId: {
            type: "string",
            description: "The node ID to get downstream lineage for",
          },
          depth: {
            type: "number",
            description: "How many levels downstream to traverse (default 3, max 6)",
          },
        },
        required: ["nodeId"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "findFlowsContaining",
      description:
        "Find pre-defined data flows that include a specific node. Flows are curated views of related tables.",
      parameters: {
        type: "object",
        properties: {
          nodeId: {
            type: "string",
            description: "The node ID to find flows for",
          },
        },
        required: ["nodeId"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "getFlowDetails",
      description: "Get detailed information about a specific flow including all member nodes.",
      parameters: {
        type: "object",
        properties: {
          flowId: {
            type: "string",
            description: "The flow ID",
          },
        },
        required: ["flowId"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "listFlows",
      description: "List all available flows in the system. Use this to see what curated views are available.",
      parameters: {
        type: "object",
        properties: {},
        required: [],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "searchByColumn",
      description:
        "Find tables/models that contain a specific column. Useful for tracing where a field originates or is used.",
      parameters: {
        type: "object",
        properties: {
          columnName: {
            type: "string",
            description: "The column name to search for (case-insensitive partial match)",
          },
          limit: {
            type: "number",
            description: "Maximum results to return (default 20)",
          },
        },
        required: ["columnName"],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "getGraphStats",
      description:
        "Get overall statistics about the data pipeline graph. Use this for general context about the size of the pipeline.",
      parameters: {
        type: "object",
        properties: {},
        required: [],
      },
    },
  },
  {
    type: "function" as const,
    function: {
      name: "searchSqlContent",
      description:
        "Search within SQL transformation code to find models that implement specific logic, use certain functions, or reference particular patterns. Use this when you need to find models by their implementation details rather than name.",
      parameters: {
        type: "object",
        properties: {
          pattern: {
            type: "string",
            description: "Search pattern - can be column names, SQL keywords (CASE WHEN, GROUP BY), function names, or partial code snippets",
          },
          limit: {
            type: "number",
            description: "Maximum results to return (default 10)",
          },
        },
        required: ["pattern"],
      },
    },
  },
];

/**
 * Execute a tool by name with the given arguments.
 */
export function executeTool(
  toolName: string,
  args: Record<string, unknown>
): unknown {
  switch (toolName) {
    case "searchNodes":
      return searchNodes(args.query as string, {
        type: args.type as string | undefined,
        limit: args.limit as number | undefined,
      });

    case "getNodeDetails":
      return getNodeDetails(args.nodeId as string);

    case "getUpstreamLineage":
      return getUpstreamLineage(
        args.nodeId as string,
        args.depth as number | undefined
      );

    case "getDownstreamLineage":
      return getDownstreamLineage(
        args.nodeId as string,
        args.depth as number | undefined
      );

    case "findFlowsContaining":
      return findFlowsContaining(args.nodeId as string);

    case "getFlowDetails":
      return getFlowDetails(args.flowId as string);

    case "listFlows":
      return listFlows();

    case "searchByColumn":
      return searchByColumn(args.columnName as string, {
        limit: args.limit as number | undefined,
      });

    case "getGraphStats":
      return getGraphStats();

    case "searchSqlContent":
      return searchSqlContent(args.pattern as string, {
        limit: args.limit as number | undefined,
      });

    default:
      throw new Error(`Unknown tool: ${toolName}`);
  }
}

import {
  connect,
  batchGetMetadata,
  disconnect,
  getAllTables,
  SnowflakeConfig,
  TableMetadata,
  TableInfo,
} from "../snowflake/client";
import { getDb } from "../db";
import type { GraphNode, GraphEdge, ColumnInfo } from "../types";
import { v4 as uuid } from "uuid";

export interface SnowflakeEnrichResult {
  enrichedCount: number;
  newNodes: GraphNode[];
  errors: string[];
}

/**
 * Enrich existing nodes with Snowflake metadata
 */
export async function enrichWithSnowflakeMetadata(
  onProgress?: (percent: number, message: string) => void
): Promise<SnowflakeEnrichResult> {
  const errors: string[] = [];
  const newNodes: GraphNode[] = [];

  // Get config from environment
  const config: SnowflakeConfig = {
    account: process.env.SNOWFLAKE_ACCOUNT || "",
    username: process.env.RIPPLING_ACCOUNT_EMAIL || "",
    authenticator: (process.env.GROWTH_SNOWFLAKE_AUTHENTICATOR as SnowflakeConfig["authenticator"]) || "externalbrowser",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || "",
    database: process.env.SNOWFLAKE_DATABASE || "PROD_RIPPLING_DWH",
    role: process.env.SNOWFLAKE_ROLE,
  };

  if (!config.account || !config.username) {
    onProgress?.(100, "Snowflake credentials not configured, skipping");
    return { enrichedCount: 0, newNodes: [], errors: ["Snowflake credentials not configured"] };
  }

  onProgress?.(5, "Connecting to Snowflake...");

  try {
    await connect(config);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    errors.push(msg);
    onProgress?.(100, `Snowflake connection failed: ${msg}`);
    return { enrichedCount: 0, newNodes: [], errors };
  }

  onProgress?.(15, "Connected. Fetching node list...");

  // Get all nodes from database
  const db = getDb();
  const nodes = db.prepare("SELECT id, name, metadata FROM nodes").all() as Array<{
    id: string;
    name: string;
    metadata: string | null;
  }>;

  // Parse FQNs to get database/schema/table
  const tablesToFetch: Array<{ database: string; schema: string; table: string; nodeId: string }> = [];

  for (const node of nodes) {
    const parts = node.id.split(".");
    if (parts.length === 3) {
      tablesToFetch.push({
        database: parts[0].toUpperCase(),
        schema: parts[1].toUpperCase(),
        table: parts[2].toUpperCase(),
        nodeId: node.id,
      });
    }
  }

  onProgress?.(25, `Fetching metadata for ${tablesToFetch.length} tables...`);

  // Batch fetch in chunks to avoid overwhelming Snowflake
  const BATCH_SIZE = 100;
  let enrichedCount = 0;

  for (let i = 0; i < tablesToFetch.length; i += BATCH_SIZE) {
    const batch = tablesToFetch.slice(i, i + BATCH_SIZE);
    const progress = 25 + Math.floor(((i / tablesToFetch.length) * 70));
    onProgress?.(progress, `Fetching batch ${Math.floor(i / BATCH_SIZE) + 1}...`);

    try {
      const metadata = await batchGetMetadata(
        batch.map((t) => ({ database: t.database, schema: t.schema, table: t.table }))
      );

      // Update nodes with metadata
      const updateStmt = db.prepare(`
        UPDATE nodes SET metadata = ? WHERE id = ?
      `);

      for (const item of batch) {
        const fqn = `${item.database}.${item.schema}.${item.table}`.toLowerCase();
        const sfMetadata = metadata.get(fqn);

        if (sfMetadata) {
          // Get existing metadata
          const node = nodes.find((n) => n.id === item.nodeId);
          const existingMetadata = node?.metadata ? JSON.parse(node.metadata) : {};

          // Merge with Snowflake metadata
          const mergedMetadata = {
            ...existingMetadata,
            columns: sfMetadata.columns.map((c) => ({
              name: c.name,
              type: c.type,
              description: c.comment,
            })) as ColumnInfo[],
            description: sfMetadata.comment || existingMetadata.description,
            snowflakeType: sfMetadata.type,
          };

          updateStmt.run(JSON.stringify(mergedMetadata), item.nodeId);
          enrichedCount++;
        }
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      errors.push(`Batch error: ${msg}`);
    }
  }

  onProgress?.(95, "Disconnecting...");
  await disconnect();

  onProgress?.(100, `Enriched ${enrichedCount} nodes with Snowflake metadata`);

  return { enrichedCount, newNodes, errors };
}

/**
 * Resolve FQN conflicts by checking Snowflake
 */
export async function resolveFqnConflicts(
  conflicts: Array<{ name: string; candidates: string[] }>
): Promise<Map<string, string>> {
  const resolutions = new Map<string, string>();

  const config: SnowflakeConfig = {
    account: process.env.SNOWFLAKE_ACCOUNT || "",
    username: process.env.RIPPLING_ACCOUNT_EMAIL || "",
    authenticator: (process.env.GROWTH_SNOWFLAKE_AUTHENTICATOR as SnowflakeConfig["authenticator"]) || "externalbrowser",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || "",
    database: process.env.SNOWFLAKE_DATABASE || "PROD_RIPPLING_DWH",
  };

  if (!config.account || !config.username) {
    return resolutions;
  }

  try {
    await connect(config);

    for (const conflict of conflicts) {
      // Check which candidates actually exist in Snowflake
      const existingCandidates: string[] = [];

      for (const candidate of conflict.candidates) {
        const parts = candidate.split(".");
        if (parts.length !== 3) continue;

        const [database, schema, table] = parts.map((p) => p.toUpperCase());

        try {
          const result = await batchGetMetadata([{ database, schema, table }]);
          if (result.size > 0) {
            existingCandidates.push(candidate);
          }
        } catch {
          // Candidate doesn't exist or we can't access it
        }
      }

      // If only one candidate exists, that's the resolution
      if (existingCandidates.length === 1) {
        resolutions.set(conflict.name, existingCandidates[0]);
      }
    }

    await disconnect();
  } catch {
    // Connection failed, return empty resolutions
  }

  return resolutions;
}

// ============================================================================
// Snowflake Table Discovery
// ============================================================================

export interface SnowflakeDiscoveryResult {
  /** New nodes discovered from Snowflake */
  nodes: GraphNode[];
  /** Edges linking discovered tables to models that reference them */
  edges: GraphEdge[];
  /** Statistics for reporting */
  stats: {
    totalTablesInSnowflake: number;
    schemasScanned: string[];
    newTablesAdded: number;
    tablesAlreadyInGraph: number;
    edgesCreated: number;
  };
  /** Any errors encountered */
  errors: string[];
  /** Whether discovery was skipped (e.g., missing credentials) */
  skipped: boolean;
  skipReason?: string;
}

/**
 * Get Snowflake configuration from environment variables
 */
export function getSnowflakeConfig(): SnowflakeConfig {
  return {
    account: process.env.SNOWFLAKE_ACCOUNT || "",
    username: process.env.RIPPLING_ACCOUNT_EMAIL || "",
    authenticator: (process.env.GROWTH_SNOWFLAKE_AUTHENTICATOR as SnowflakeConfig["authenticator"]) || "externalbrowser",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || "",
    database: process.env.SNOWFLAKE_DATABASE || "PROD_RIPPLING_DWH",
    role: process.env.SNOWFLAKE_ROLE,
  };
}

/**
 * Check if Snowflake credentials are configured
 */
export function hasSnowflakeCredentials(): boolean {
  const config = getSnowflakeConfig();
  return !!(config.account && config.username);
}

/**
 * Discover all tables in Snowflake that aren't already in the graph.
 * Creates nodes for new tables and optionally links them to models that reference them.
 * 
 * @param existingNodeIds - Set of node IDs already in the graph
 * @param existingNodes - Array of existing nodes (for SQL content scanning)
 * @param onProgress - Progress callback
 * @param selectedSchemas - Optional list of schemas to include (if not provided, discovers all)
 */
export async function discoverSnowflakeTables(
  existingNodeIds: Set<string>,
  existingNodes: GraphNode[],
  onProgress?: (percent: number, message: string) => void,
  selectedSchemas?: string[]
): Promise<SnowflakeDiscoveryResult> {
  const result: SnowflakeDiscoveryResult = {
    nodes: [],
    edges: [],
    stats: {
      totalTablesInSnowflake: 0,
      schemasScanned: [],
      newTablesAdded: 0,
      tablesAlreadyInGraph: 0,
      edgesCreated: 0,
    },
    errors: [],
    skipped: false,
  };

  const config = getSnowflakeConfig();

  // Check for credentials
  if (!config.account || !config.username) {
    result.skipped = true;
    result.skipReason = "SNOWFLAKE_ACCOUNT or RIPPLING_ACCOUNT_EMAIL not configured";
    onProgress?.(100, `⚠️ Snowflake discovery skipped: ${result.skipReason}`);
    return result;
  }

  onProgress?.(5, "Connecting to Snowflake...");

  // Connect to Snowflake
  try {
    await connect(config);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    result.skipped = true;
    result.skipReason = `Connection failed: ${msg}`;
    result.errors.push(msg);
    onProgress?.(100, `⚠️ Snowflake discovery skipped: ${result.skipReason}`);
    return result;
  }

  onProgress?.(10, "Querying tables from Snowflake...");

  // Get all tables from Snowflake
  let allTables: TableInfo[] = [];
  try {
    allTables = await getAllTables(config.database, ["_DEV", "_TEST"]);
    
    // Filter by selected schemas if provided
    if (selectedSchemas && selectedSchemas.length > 0) {
      const selectedSet = new Set(selectedSchemas.map(s => s.toUpperCase()));
      allTables = allTables.filter(t => selectedSet.has(t.schema.toUpperCase()));
    }
    
    result.stats.totalTablesInSnowflake = allTables.length;

    // Collect unique schemas
    const schemas = new Set(allTables.map((t) => t.schema));
    result.stats.schemasScanned = [...schemas].sort();

    const schemaMsg = selectedSchemas 
      ? `Found ${allTables.length} tables in ${schemas.size} selected schemas`
      : `Found ${allTables.length} tables across ${schemas.size} schemas`;
    onProgress?.(20, schemaMsg);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    result.errors.push(`Failed to query tables: ${msg}`);
    onProgress?.(100, `⚠️ Snowflake discovery failed: ${msg}`);
    await disconnect();
    return result;
  }

  // Filter to only tables not already in the graph
  const newTables: TableInfo[] = [];
  for (const table of allTables) {
    const fqn = `${table.database}.${table.schema}.${table.name}`.toLowerCase();
    if (existingNodeIds.has(fqn)) {
      result.stats.tablesAlreadyInGraph++;
    } else {
      newTables.push(table);
    }
  }

  onProgress?.(25, `Discovered ${newTables.length} new tables (${result.stats.tablesAlreadyInGraph} already in graph)`);

  if (newTables.length === 0) {
    onProgress?.(100, "No new tables to add from Snowflake");
    await disconnect();
    return result;
  }

  // Batch fetch column metadata for new tables
  const BATCH_SIZE = 100;
  const tableMetadataMap = new Map<string, TableMetadata>();

  for (let i = 0; i < newTables.length; i += BATCH_SIZE) {
    const batch = newTables.slice(i, i + BATCH_SIZE);
    const progress = 25 + Math.floor(((i / newTables.length) * 50));
    onProgress?.(progress, `Fetching column metadata: batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(newTables.length / BATCH_SIZE)}`);

    try {
      const metadata = await batchGetMetadata(
        batch.map((t) => ({
          database: t.database,
          schema: t.schema,
          table: t.name,
        }))
      );

      // Merge into our map
      for (const [fqn, meta] of metadata) {
        tableMetadataMap.set(fqn, meta);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      result.errors.push(`Batch metadata error: ${msg}`);
    }
  }

  onProgress?.(75, "Creating nodes for discovered tables...");

  // Create GraphNode for each new table
  for (const table of newTables) {
    const fqn = `${table.database}.${table.schema}.${table.name}`.toLowerCase();
    const metadata = tableMetadataMap.get(fqn);

    const node: GraphNode = {
      id: fqn,
      name: table.name,
      type: "source",
      subtype: "snowflake_raw",
      repo: "snowflake",
      metadata: {
        description: metadata?.comment || `Raw Snowflake ${table.type.toLowerCase()} discovered via Snowflake integration`,
        columns: metadata?.columns.map((c) => ({
          name: c.name,
          type: c.type,
          description: c.comment,
        })) as ColumnInfo[] || [],
        snowflakeType: table.type,
        snowflakeSchema: table.schema,
      },
      semanticLayer: "source",
    };

    result.nodes.push(node);
    result.stats.newTablesAdded++;
  }

  onProgress?.(85, "Scanning for SQL references to link tables...");

  // Scan existing model SQL for references to discovered tables
  const edges = findSqlReferences(result.nodes, existingNodes);
  result.edges = edges;
  result.stats.edgesCreated = edges.length;

  onProgress?.(95, "Disconnecting from Snowflake...");
  await disconnect();

  onProgress?.(100, `Added ${result.stats.newTablesAdded} tables from Snowflake, created ${result.stats.edgesCreated} edges`);

  return result;
}

/**
 * Scan existing model SQL content for references to discovered tables.
 * Creates edges from discovered tables to models that reference them.
 */
function findSqlReferences(
  discoveredNodes: GraphNode[],
  existingNodes: GraphNode[]
): GraphEdge[] {
  const edges: GraphEdge[] = [];

  // Build lookup maps for discovered tables
  const tablesByFqn = new Map<string, GraphNode>();
  const tablesBySchemaTable = new Map<string, GraphNode>();
  const tablesByName = new Map<string, GraphNode[]>();

  for (const node of discoveredNodes) {
    const parts = node.id.split(".");
    if (parts.length === 3) {
      const [db, schema, table] = parts;
      tablesByFqn.set(node.id.toLowerCase(), node);
      tablesBySchemaTable.set(`${schema}.${table}`.toLowerCase(), node);

      // Track by short name (may have duplicates)
      const tableLower = table.toLowerCase();
      if (!tablesByName.has(tableLower)) {
        tablesByName.set(tableLower, []);
      }
      tablesByName.get(tableLower)!.push(node);
    }
  }

  // Scan each existing node's SQL content
  for (const existingNode of existingNodes) {
    const sql = existingNode.sqlContent;
    if (!sql) continue;

    const sqlLower = sql.toLowerCase();
    const matchedTables = new Set<string>();

    // Check for FQN references (database.schema.table)
    for (const [fqn, discoveredNode] of tablesByFqn) {
      // Look for the FQN in SQL (case-insensitive)
      if (sqlLower.includes(fqn)) {
        matchedTables.add(discoveredNode.id);
      }
    }

    // Check for schema.table references
    for (const [schemaTable, discoveredNode] of tablesBySchemaTable) {
      if (sqlLower.includes(schemaTable) && !matchedTables.has(discoveredNode.id)) {
        matchedTables.add(discoveredNode.id);
      }
    }

    // Check for short table name references (only if unambiguous)
    for (const [tableName, nodes] of tablesByName) {
      // Only match if there's exactly one table with this name to avoid ambiguity
      if (nodes.length === 1) {
        // Use word boundary matching to avoid partial matches
        const pattern = new RegExp(`\\b${tableName}\\b`, "i");
        if (pattern.test(sql) && !matchedTables.has(nodes[0].id)) {
          matchedTables.add(nodes[0].id);
        }
      }
    }

    // Create edges for all matches
    for (const discoveredTableId of matchedTables) {
      edges.push({
        id: uuid(),
        from: discoveredTableId,
        to: existingNode.id,
        type: "sql_dependency",
        metadata: {
          transformationType: "discovered-reference",
          inferredFrom: "sql-scan",
        },
      });
    }
  }

  return edges;
}


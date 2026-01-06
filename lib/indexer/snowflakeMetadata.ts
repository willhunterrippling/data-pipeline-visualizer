import {
  connect,
  batchGetMetadata,
  disconnect,
  SnowflakeConfig,
  TableMetadata,
} from "../snowflake/client";
import { getDb } from "../db";
import type { GraphNode, ColumnInfo } from "../types";

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

